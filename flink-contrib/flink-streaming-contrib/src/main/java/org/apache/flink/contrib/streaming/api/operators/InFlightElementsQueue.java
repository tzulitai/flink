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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A queue of {@link InFlightElement}s. Please see the class documentation of {@link InFlightElement} on why elements
 * may be buffered.
 *
 * <p>This class also serves as the internal operator state controller for a {@link StreamMultiThreadedFlatMap}. Any
 * manipulation to the queue, (i.e. adding in-flight elements, collecting outputs, removing elements to be emitted,
 * snapshotting the current queue state etc.) must therefore be synchronized on the checkpoint lock.
 */
public class InFlightElementsQueue<OUT> {

	private final Object checkpointLock;
	private final Map<Long, InFlightElement<OUT>> queue;

	private long indexCounter;

	public InFlightElementsQueue(Object checkpointLock) {
		this.checkpointLock = checkNotNull(checkpointLock);
		this.queue = new LinkedHashMap<>();
		this.indexCounter = Long.MIN_VALUE;
	}

	public long add(StreamElement element) {
		assert Thread.holdsLock(checkpointLock);
		long index = indexCounter++;
		queue.put(index, new InFlightElement<OUT>(element));

		// check if there is emittable outputs
		// if yes,
		checkpointLock.notifyAll();

		return index;
	}

	public void setOutputsForElement(long index, Collection<OUT> outputs) {
		assert Thread.holdsLock(checkpointLock);

		// check if there is emittable outputs
		// if yes,
		checkpointLock.notifyAll();

		queue.get(index).setOutputCollection(outputs);
	}

	/**
	 * Blocks
	 *
	 * @return
	 */
	public Collection<InFlightElement<OUT>> fetchAndRemoveEmittableElements() throws InterruptedException {
		assert Thread.holdsLock(checkpointLock);

		// if no emittable outputs, block until we find out there are elements that can be emitted
		if (!hasEmittableElements()) {
			checkpointLock.wait();
		}

		List<InFlightElement<OUT>> emittableELements = new LinkedList<>();
		boolean seenIncompleteRecordElement = false;

		Iterator<Map.Entry<Long, InFlightElement<OUT>>> queueIterator = queue.entrySet().iterator();
		while (queueIterator.hasNext()) {
			InFlightElement element = queueIterator.next().getValue();
			StreamElement streamElement = element.getElement();
			if (streamElement.isRecord()) {
				if (element.isOutputCollected()) {
					emittableELements.add(element);
					queueIterator.remove();
				} else {
					seenIncompleteRecordElement = true;
				}
			} else {
				if (!seenIncompleteRecordElement) {
					emittableELements.add(element);
					queueIterator.remove();
				} else {
					break;
				}
			}
		}

		return emittableELements;
	}

	public List<InFlightElement<OUT>> snapshotElements() {
		assert Thread.holdsLock(checkpointLock);

		List<InFlightElement<OUT>> snapshot = new ArrayList<>(queue.size());
		for (Map.Entry<Long, InFlightElement<OUT>> element : queue.entrySet()) {
			snapshot.add(element.getValue());
		}

		return snapshot;
	}

	@VisibleForTesting
	boolean hasEmittableElements() {
		if (queue.size() == 0) {
			return false;
		} else {
			int numRecordElementsSeen = 0;

			for (InFlightElement queueEntry : queue.values()) {
				StreamElement streamElement = queueEntry.getElement();
				if (streamElement.isRecord()) {
					numRecordElementsSeen++;
					if (queueEntry.isOutputCollected()) {
						return true;
					} else {
						numRecordElementsSeen++;
					}
				} else {
					return numRecordElementsSeen == 0;
				}
			}

			// silence compiler
			return false;
		}
	}
}
