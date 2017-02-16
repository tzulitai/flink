/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ScheduledFuture;

/**
 * Source contexts for various stream time characteristics.
 */
public class StreamSourceContexts {

	/**
	 * Depending on the {@link TimeCharacteristic}, this method will return the adequate
	 * {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}. That is:
	 * <ul>
	 *     <li>{@link TimeCharacteristic#IngestionTime} = {@link AutomaticWatermarkContext}</li>
	 *     <li>{@link TimeCharacteristic#ProcessingTime} = {@link NonTimestampContext}</li>
	 *     <li>{@link TimeCharacteristic#EventTime} = {@link ManualWatermarkContext}</li>
	 * </ul>
	 * */
	public static <OUT> SourceFunction.SourceContext<OUT> getSourceContext(
			TimeCharacteristic timeCharacteristic,
			ProcessingTimeService processingTimeService,
			Object checkpointLock,
			StreamStatusMaintainer streamStatusMaintainer,
			Output<StreamRecord<OUT>> output,
			long watermarkInterval,
			long idleTimeout) {

		final SourceFunction.SourceContext<OUT> ctx;
		switch (timeCharacteristic) {
			case EventTime:
				ctx = new ManualWatermarkContext<>(
					output,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);

				break;
			case IngestionTime:
				ctx = new AutomaticWatermarkContext<>(
					output,
					watermarkInterval,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);

				break;
			case ProcessingTime:
				ctx = new NonTimestampContext<>(checkpointLock, output);
				break;
			default:
				throw new IllegalArgumentException(String.valueOf(timeCharacteristic));
		}
		return ctx;
	}

	/**
	 * A source context that attached {@code -1} as a timestamp to all records, and that
	 * does not forward watermarks.
	 */
	private static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lock;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private NonTimestampContext(Object checkpointLock, Output<StreamRecord<T>> output) {
			this.lock = Preconditions.checkNotNull(checkpointLock, "The checkpoint lock cannot be null.");
			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		public void collect(T element) {
			synchronized (lock) {
				output.collect(reuse.replace(element));
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			// ignore the timestamp
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			// do nothing
		}

		@Override
		public void markAsTemporarilyIdle() {
			// do nothing
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
	 * and watermark emission.
	 */
	private static class AutomaticWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private final long watermarkInterval;

		private volatile ScheduledFuture<?> nextWatermarkTimer;
		private volatile long nextWatermarkTime;

		private long lastRecordTime;

		private AutomaticWatermarkContext(
				final Output<StreamRecord<T>> output,
				final long watermarkInterval,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");

			Preconditions.checkArgument(watermarkInterval >= 1L, "The watermark interval cannot be smaller than 1 ms.");
			this.watermarkInterval = watermarkInterval;

			this.reuse = new StreamRecord<>(null);

			this.lastRecordTime = Long.MIN_VALUE;

			long now = this.timeService.getCurrentProcessingTime();
			this.nextWatermarkTimer = this.timeService.registerTimer(now + watermarkInterval,
				new WatermarkEmittingTask(this.timeService, checkpointLock, output));
		}

		@Override
		protected void processAndCollect(T element) {
			lastRecordTime = this.timeService.getCurrentProcessingTime();
			output.collect(reuse.replace(element, lastRecordTime));

			// this is to avoid lock contention in the lockingObject by
			// sending the watermark before the firing of the watermark
			// emission task.
			if (lastRecordTime > nextWatermarkTime) {
				// in case we jumped some watermarks, recompute the next watermark time
				final long watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
				nextWatermarkTime = watermarkTime + watermarkInterval;
				output.emitWatermark(new Watermark(watermarkTime));

				// we do not need to register another timer here
				// because the emitting task will do so.
			}
		}

		@Override
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			processAndCollect(element);
		}

		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			nextWatermarkTime = Long.MAX_VALUE;
			output.emitWatermark(mark);

			// we can shutdown the watermark timer now, no watermarks will be needed any more.
			// Note that this procedure actually doesn't need to be synchronized with the lock,
			// but since it's only a one-time thing, doesn't hurt either
			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		@Override
		protected boolean allowWatermark(Watermark mark) {
			// allow it since this is the special end-watermark that for example the Kafka source emits
			return mark.getTimestamp() == Long.MAX_VALUE && nextWatermarkTime != Long.MAX_VALUE;
		}

		@Override
		public void close() {
			super.close();

			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		private class WatermarkEmittingTask implements ProcessingTimeCallback {

			private final ProcessingTimeService timeService;
			private final Object lock;
			private final Output<StreamRecord<T>> output;

			private WatermarkEmittingTask(
					ProcessingTimeService timeService,
					Object checkpointLock,
					Output<StreamRecord<T>> output) {
				this.timeService = timeService;
				this.lock = checkpointLock;
				this.output = output;
			}

			@Override
			public void onProcessingTime(long timestamp) {
				final long currentTime = timeService.getCurrentProcessingTime();

				if (currentTime > nextWatermarkTime) {
					// align the watermarks across all machines. this will ensure that we
					// don't have watermarks that creep along at different intervals because
					// the machine clocks are out of sync
					final long watermarkTime = currentTime - (currentTime % watermarkInterval);

					synchronized (lock) {
						if (currentTime > nextWatermarkTime) {
							output.emitWatermark(new Watermark(watermarkTime));
							nextWatermarkTime = watermarkTime + watermarkInterval;
						}

						// piggy-back the source idle check on the watermark interval, so that we may
						// possibly discover idle sources faster before waiting for the next idle check to fire
						if (streamStatusMaintainer.getStreamStatus().isActive() && currentTime - lastRecordTime > idleTimeout) {
							markAsTemporarilyIdle();
						}
					}
				}

				long nextWatermark = currentTime + watermarkInterval;
				nextWatermarkTimer = this.timeService.registerTimer(
						nextWatermark, new WatermarkEmittingTask(this.timeService, lock, output));
			}
		}
	}

	/**
	 * A SourceContext for event time. Sources may directly attach timestamps and generate
	 * watermarks, but if records are emitted without timestamps, no timestamps are automatically
	 * generated and attached. The records will simply have no timestamp in that case.
	 *
	 * Streaming topologies can use timestamp assigner functions to override the timestamps
	 * assigned here.
	 */
	private static class ManualWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private ManualWatermarkContext(
				final Output<StreamRecord<T>> output,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		protected void processAndCollect(T element) {
			output.collect(reuse.replace(element));
		}

		@Override
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			output.collect(reuse.replace(element, timestamp));
		}

		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		protected boolean allowWatermark(Watermark mark) {
			return true;
		}
	}

	/**
	 *
	 * @param <T>
	 */
	private static abstract class WatermarkContext<T> implements SourceFunction.SourceContext<T> {

		protected final ProcessingTimeService timeService;

		protected final Object checkpointLock;
		protected final StreamStatusMaintainer streamStatusMaintainer;
		protected final long idleTimeout;

		private ScheduledFuture<?> nextCheck;

		private volatile boolean failOnNextCheck;

		public WatermarkContext(
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			this.timeService = Preconditions.checkNotNull(timeService, "Time Service cannot be null.");
			this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "Checkpoint Lock cannot be null.");
			this.streamStatusMaintainer = Preconditions.checkNotNull(streamStatusMaintainer, "Stream Status Maintainer cannot be null.");

			Preconditions.checkArgument(idleTimeout >= 0, "The idle timeout cannot be smaller than 0 ms.");
			this.idleTimeout = idleTimeout;
		}

		@Override
		public void collect(T element) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					triggerNextIdleCheckTimer();
				}

				processAndCollect(element);
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					triggerNextIdleCheckTimer();
				}

				processAndCollectWithTimestamp(element, timestamp);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			if (allowWatermark(mark)) {
				synchronized (checkpointLock) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

					if (nextCheck != null) {
						this.failOnNextCheck = false;
					} else {
						triggerNextIdleCheckTimer();
					}

					processAndEmitWatermark(mark);
				}
			}
		}

		@Override
		public void markAsTemporarilyIdle() {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
			}
		}

		@Override
		public Object getCheckpointLock() {
			return checkpointLock;
		}

		@Override
		public void close() {
			final ScheduledFuture<?> nextCheck = this.nextCheck;
			if (nextCheck != null) {
				nextCheck.cancel(true);
			}
		}

		private void triggerNextIdleCheckTimer() {
			if (idleTimeout > 0) {
				failOnNextCheck = true;
				nextCheck = this.timeService.registerTimer(
					this.timeService.getCurrentProcessingTime() + idleTimeout,
					new StreamStatusCheckTask());
			}
		}

		private class StreamStatusCheckTask implements ProcessingTimeCallback {
			@Override
			public void onProcessingTime(long timestamp) throws Exception {
				synchronized (checkpointLock) {
					nextCheck = null;

					if (failOnNextCheck) {
						markAsTemporarilyIdle();
					} else {
						triggerNextIdleCheckTimer();
					}
				}
			}
		}

		// ------------------------------------------------------------------------
		//	Abstract methods for concrete subclasses to implement.
		//  These methods are guaranteed to be synchronized on the checkpoint lock,
		//  so implementations don't need to do so.
		// ------------------------------------------------------------------------

		protected abstract void processAndCollect(T element);
		protected abstract void processAndCollectWithTimestamp(T element, long timestamp);

		/** Whether or not a watermark should be allowed */
		protected abstract boolean allowWatermark(Watermark mark);

		/** Process and emit watermark. Only called if {@link WatermarkContext#allowWatermark(Watermark)} returns {@code true} */
		protected abstract void processAndEmitWatermark(Watermark mark);

	}
}
