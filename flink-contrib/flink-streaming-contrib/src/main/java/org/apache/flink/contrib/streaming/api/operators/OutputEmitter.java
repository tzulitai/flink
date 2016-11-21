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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A thread that actually emits outputs downstream. The thread works only on the {@link InFlightElementsQueue}, fetching
 * and removing outputs from the queue that can be emitted.
 */
public class OutputEmitter<OUT> extends Thread {

	private final InFlightElementsQueue<OUT> queue;
	private final TimestampedCollector<OUT> timestampedCollector;
	private final Output<StreamRecord<OUT>> output;
	private final Object checkpointLock;

	private volatile boolean running = true;

	public OutputEmitter(InFlightElementsQueue<OUT> queue,
						Object checkpointLock,
						Output<StreamRecord<OUT>> output) {
		this.queue = checkNotNull(queue);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.output = checkNotNull(output);
		this.timestampedCollector = new TimestampedCollector<>(output);
	}

	@Override
	public void run() {
		try {
			while (running) {
				synchronized (checkpointLock) {
					Collection<InFlightElement<OUT>> elementsToEmit = queue.fetchAndRemoveEmittableElements();

					for (InFlightElement<OUT> element : elementsToEmit) {
						StreamElement streamElement = element.getElement();

						if (streamElement.isRecord()) {
							for (OUT outputValue : element.getOutputs()) {
								timestampedCollector.collect(outputValue);
							}
						} else if (streamElement.isWatermark()) {
							output.emitWatermark(streamElement.asWatermark());
						} else if (streamElement.isLatencyMarker()) {
							output.emitLatencyMarker(streamElement.asLatencyMarker());
						} else {
							throw new RuntimeException("Unrecognized :" );
						}
					}
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void shutdown() {
		running = false;
	}
}
