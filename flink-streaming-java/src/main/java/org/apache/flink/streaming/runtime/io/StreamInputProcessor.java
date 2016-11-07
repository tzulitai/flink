/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.WatermarkStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link Watermark} events and forwards them to event subscribers
 * once the {@link Watermark} from all inputs advances.
 *
 * <p>
 * Forwarding elements or watermarks must be protected by synchronizing on the given lock
 * object. This ensures that we don't call methods on a {@link OneInputStreamOperator} concurrently
 * with the timer callback or other things.
 * 
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final CheckpointBarrierHandler barrierHandler;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks and watermark statuses to input channels
	private int currentChannel = -1;

	/** Status of each input channel */
	private final InputChannelStatus[] channelStatuses;

	private long lastEmittedWatermark;

	/** Overall watermark status, considered active if at least 1 input channel is active, idle otherwise */
	private WatermarkStatus currentWatermarkStatus;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private Counter numRecordsIn;

	private boolean isFinished;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
			InputGate[] inputGates,
			TypeSerializer<IN> inputSerializer,
			StatefulTask checkpointedTask,
			CheckpointingMode checkpointMode,
			IOManager ioManager) throws IOException {

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate);
		}
		else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
		
		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}
		
		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		channelStatuses = new InputChannelStatus[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			channelStatuses[i] = new InputChannelStatus(Long.MIN_VALUE, WatermarkStatus.ACTIVE, true);
		}

		lastEmittedWatermark = Long.MIN_VALUE;
		currentWatermarkStatus = WatermarkStatus.ACTIVE;
	}

	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
	public boolean processInput(OneInputStreamOperator<IN, ?> streamOperator, final Object lock) throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						handleWatermark(streamOperator, recordOrMark.asWatermark(), currentChannel, lock);
						continue;
					} else if (recordOrMark.isWatermarkStatus()) {
						handleWatermarkStatus(streamOperator, recordOrMark.asWatermarkStatus(), currentChannel, lock);
						continue;
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	/**
	 * Sets the metric group for this StreamInputProcessor.
	 * 
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return lastEmittedWatermark;
			}
		});

		metrics.gauge("checkpointAlignmentTime", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return barrierHandler.getAlignmentDurationNanos();
			}
		});
	}
	
	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
		
		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
	private void handleWatermark(OneInputStreamOperator<IN, ?> operator,
								 Watermark mark,
								 int channelIndex,
								 Object lock) throws Exception {
		// we should not be receiving watermarks if the overall watermark status is idle, or
		// the input channel that propagated the watermark is idle
		if (currentWatermarkStatus.isIdle() || channelStatuses[channelIndex].isWatermarkActive()) {
			throw new IllegalStateException("Stream input processor received a watermark while " +
				"overall / channel watermark status was marked as: " + currentWatermarkStatus + " / " +
				channelStatuses[channelIndex].getWatermarkStatus());
		} else {
			long watermarkMillis = mark.getTimestamp();
			if (watermarkMillis > channelStatuses[channelIndex].getWatermark()) {
				channelStatuses[channelIndex].setWatermark(watermarkMillis);

				// previously unaligned channels are now aligned if its watermark has caught up
				if (!channelStatuses[channelIndex].isWatermarkAligned() &&
					watermarkMillis >= lastEmittedWatermark) {
					channelStatuses[channelIndex].setIsWatermarkAligned(true);
				}

				// advance overall watermark by considering only aligned channels across all channels
				long newMinWatermark = Long.MAX_VALUE;
				for (InputChannelStatus channelStatus : channelStatuses) {
					if (!channelStatus.isWatermarkAligned()) {
						newMinWatermark = Math.min(channelStatus.getWatermark(), newMinWatermark);
					}
				}
				if (newMinWatermark > lastEmittedWatermark) {
					lastEmittedWatermark = newMinWatermark;
					synchronized (lock) {
						operator.processWatermark(new Watermark(lastEmittedWatermark));
					}
				}
			}
		}
	}

	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
	private void handleWatermarkStatus(OneInputStreamOperator<IN, ?> operator,
									   WatermarkStatus watermarkStatus,
									   int channelIndex,
									   Object lock) throws Exception {
		if (watermarkStatus.isIdle() && channelStatuses[channelIndex].isWatermarkActive()) {
			// handle watermark idle status
			channelStatuses[channelIndex].setWatermarkStatus(WatermarkStatus.IDLE);

			// the channel is now idle, therefore not aligned
			channelStatuses[channelIndex].setIsWatermarkAligned(false);

			if (!hasWatermarkActiveChannels(channelStatuses)) {
				currentWatermarkStatus = WatermarkStatus.IDLE;
				synchronized (lock) {
					operator.processWatermarkStatus(currentWatermarkStatus);
				}
			}
		} else if (watermarkStatus.isActive() && channelStatuses[channelIndex].isWatermarkIdle()) {
			// handle watermark active status
			channelStatuses[channelIndex].setWatermarkStatus(WatermarkStatus.ACTIVE);

			// if the last watermark of the channel, before it was marked idle, is still
			// larger than the overall last emitted watermark, then we can set the channel to
			// be aligned already.
			if (channelStatuses[channelIndex].getWatermark() >= lastEmittedWatermark) {
				channelStatuses[channelIndex].setIsWatermarkAligned(true);
			}

			// propagate active status if we are transisting back to active from idle
			if (currentWatermarkStatus.isIdle()) {
				currentWatermarkStatus = WatermarkStatus.ACTIVE;
				synchronized (lock) {
					operator.processWatermarkStatus(currentWatermarkStatus);
				}
			}
		}
	}

	private static boolean hasWatermarkActiveChannels(InputChannelStatus[] statuses) {
		for (InputChannelStatus status : statuses) {
			if (status.isWatermarkActive()) {
				return true;
			}
		}
		return false;
	}
}
