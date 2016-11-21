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

package org.apache.flink.contrib.streaming.api.operators;

import org.apache.flink.contrib.streaming.api.functions.MultiThreadedFlatMapFunction;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Operator for {@link MultiThreadedFlatMapFunction}. For each input stream record, the function invocation is submitted
 * as a concurrent task to a user-defined fixed-size thread pool.
 *
 * <p>To coordinate with checkpointing, a queue of in-flight elements is maintained as the operator's internal state,
 * which is checkpointed along with the user-state. Please see the class documentation of {@link InFlightElement} and
 * {@link InFlightElementsQueue} for more detail.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public class StreamMultiThreadedFlatMap<IN, OUT>
	extends AbstractUdfStreamOperator<OUT, MultiThreadedFlatMapFunction<IN, OUT>>
	implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private final int threadPoolSize;

	private transient List<InFlightElement<OUT>> restoredInFlightElements;
	private transient BoundedThreadPoolExecutor executor;
	private transient Object checkpointLock;
	private transient StreamElementSerializer<IN> inSerializer;
	private transient StreamElementSerializer<OUT> outSerializer;
	private transient InFlightElementsQueue<OUT> queue;
	private transient OutputEmitter outputEmitter;

	public StreamMultiThreadedFlatMap(MultiThreadedFlatMapFunction<IN, OUT> function, int threadPoolSize) {
		super(function);
		chainingStrategy = ChainingStrategy.ALWAYS;
		checkArgument(threadPoolSize > 0);
		this.threadPoolSize = threadPoolSize;
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		containingTask.getCheckpointLock();

		this.inSerializer = new StreamElementSerializer<>(config.<IN>getTypeSerializerIn1(getUserCodeClassloader()));
		this.outSerializer = new StreamElementSerializer<>(config.<OUT>getTypeSerializerOut(getUserCodeClassloader()));

		this.executor = new BoundedThreadPoolExecutor(threadPoolSize);
		this.checkpointLock = containingTask.getCheckpointLock();
		this.queue = new InFlightElementsQueue<>(this.checkpointLock);

		this.outputEmitter = new OutputEmitter<>(this.queue, this.checkpointLock, output);
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (this.restoredInFlightElements != null) {
			for (InFlightElement element : this.restoredInFlightElements) {
				long index = this.queue.add(element.getElement());
				// re-invoke function
				if (!element.isOutputCollected()) {
					this.executor.submitConcurrentFlatMapInvoke(
						element.getElement().<IN>asRecord().getValue(),
						checkpointLock,
						index);
				}
			}
		}
		this.outputEmitter.start();
	}

	@Override
	public void close() throws Exception {
		super.close();

		executor.terminate();
		outputEmitter.interrupt();
		outputEmitter.shutdown();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		executor.terminate();
		outputEmitter.interrupt();
		outputEmitter.shutdown();
	}

	// ------------------------------------------------------------------------
	//  Stream element handling
	// ------------------------------------------------------------------------

	@Override
	@SuppressWarnings("SynchronizeOnNonFinalField")
	public void processElement(final StreamRecord<IN> element) throws Exception {
		long index = queue.add(element);
		executor.submitConcurrentFlatMapInvoke(element.getValue(), checkpointLock, index);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		processNonRecordElement(mark);
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		processNonRecordElement(latencyMarker);
	}

	// ------------------------------------------------------------------------
	//  State snapshotting & restoring
	// ------------------------------------------------------------------------

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		super.snapshotState(out, checkpointId, timestamp);
		serializeInFlightElements(queue.snapshotElements(), out);
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		super.restoreState(in);
		this.restoredInFlightElements = deserlializeInFlightElements(in);
	}

	private void serializeInFlightElements(List<InFlightElement<OUT>> elements,
										FSDataOutputStream outStream) throws IOException {
		DataOutputView view = new DataOutputViewStreamWrapper(outStream);

		view.writeInt(elements.size());

		for (InFlightElement<OUT> element : elements) {
			InFlightElement.serialize(element, view, inSerializer, outSerializer);
		}
	}

	private List<InFlightElement<OUT>> deserlializeInFlightElements(FSDataInputStream inStream) throws IOException {
		DataInputView view = new DataInputViewStreamWrapper(inStream);

		int numElements = view.readInt();

		List<InFlightElement<OUT>> deserialized = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			deserialized.add(InFlightElement.deserialize(view, inSerializer, outSerializer));
		}

		return deserialized;
	}

	@SuppressWarnings("SynchronizeOnNonFinalField")
	private void processNonRecordElement(StreamElement element) {
		if (executor.getActiveThreadsCount() == 0) {
			if (element.isWatermark()) {
				output.emitWatermark(element.asWatermark());
			} else if (element.isLatencyMarker()) {
				output.emitLatencyMarker(element.asLatencyMarker());
			} else {
				throw new RuntimeException("Unrecognized element: " + element);
			}
		} else {
			queue.add(element);
		}
	}

	private class BoundedThreadPoolExecutor {

		private final ThreadPoolExecutor executor;
		private final Semaphore semaphore;

		BoundedThreadPoolExecutor(int threadPoolSize) {
			checkArgument(threadPoolSize > 0);
			this.semaphore = new Semaphore(threadPoolSize);
			this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
		}

		void submitConcurrentFlatMapInvoke(final IN value, final Object checkpointLock, final long queueIndex)
			throws InterruptedException {
			semaphore.acquire();

			executor.submit(new Runnable() {
				@Override
				public void run() {
					try {
						userFunction.flatMap(value, checkpointLock, new ExactlyOnceCollector<>(queue, queueIndex, checkpointLock));
					} catch (Exception e) {
						throw new RuntimeException("Exception in task: " + e);
					} finally {
						semaphore.release();
					}
				}
			});
		}

		void terminate() {
			executor.shutdownNow();
		}

		int getActiveThreadsCount() {
			return executor.getActiveCount();
		}

	}
}
