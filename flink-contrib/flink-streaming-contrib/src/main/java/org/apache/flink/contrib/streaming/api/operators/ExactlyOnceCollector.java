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

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@code ExactlyOnceCollector} is provided to each concurrent flatMap invocation.
 * It serves as a handover of the transformed outputs to the {@link InFlightElementsQueue}.
 */
public class ExactlyOnceCollector<OUT> {

	private final InFlightElementsQueue<OUT> queue;
	private final long inputRecordIndex;
	private final Object checkpointLock;

	public ExactlyOnceCollector(InFlightElementsQueue<OUT> queue,
								long inputRecordIndex,
								Object checkpointLock) {
		this.queue = checkNotNull(queue);
		this.inputRecordIndex = inputRecordIndex;
		this.checkpointLock = checkNotNull(checkpointLock);
	}

	/**
	 * This method should be called exactly one time to collect transformed outputs.
	 * If there are no transformed outputs, you should still provide an empty collection.
	 *
	 * @param outputs the output collection to collect.
	 */
	public void collect(Collection<OUT> outputs) {
		synchronized (checkpointLock) {
			queue.setOutputsForElement(inputRecordIndex, outputs);
		}
	}

}
