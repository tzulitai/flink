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

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
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

	/**
	 * The checkpoint lock is used for several purposes in this class:
	 *  1. Synchronize all manipulation and access to the queue with checkpointing
	 *  2. Block calling thread of adding elements if queue is currently saturated
	 *  3. Block calling thread of removing elements if no elements can be removed
	 *
	 * We cannot use a separate lock for 2 and 3, because that may cause in deadlocking checkpointing.
	 */
	private final Object checkpointLock;

	/** The maximum allowed number of in-flight elements (queue buffer size) */
	private final int maxNumInFlightElements;

	private final Map<Long, InFlightElement<OUT>> queue;

	private long indexCounter;

	public InFlightElementsQueue(int maxNumInFlightElements, Object checkpointLock) {
		checkArgument(maxNumInFlightElements > 0);
		this.maxNumInFlightElements = maxNumInFlightElements;
		this.checkpointLock = checkNotNull(checkpointLock);
		this.queue = new LinkedHashMap<>();
		this.indexCounter = Long.MIN_VALUE;
	}

	public long add(StreamElement element) throws InterruptedException {
		return add(new InFlightElement<OUT>(element));
	}

	public long add(InFlightElement<OUT> inFlightElement) throws InterruptedException {
		synchronized (checkpointLock) {
			// block calling thread until the queue has space;
			// this can only be woken up by "removeNextEmittableElement()"
			if (queue.size() >= maxNumInFlightElements) {
				checkpointLock.wait();
			}

			long index = indexCounter++;
			queue.put(index, inFlightElement);

			// if the added element results in the queue to contain elements that are qualified
			// to be emitted, this wakes up blocking callers on "removeNextEmittableElement()"
			if (hasEmittableElements()) {
				checkpointLock.notifyAll();
			}

			return index;
		}
	}

	public void setOutputsForElement(long index, Collection<OUT> outputs) {
		synchronized (checkpointLock) {
			queue.get(index).setOutputCollection(outputs);

			// if the added element results in the queue to contain elements that are qualified
			// to be emitted, this wakes up blocking callers on "removeNextEmittableElement()"
			if (hasEmittableElements()) {
				checkpointLock.notifyAll();
			}
		}
	}

	/**
	 * Removes and returns the next emittable element from the queue. Please see the class documentation of
	 * {@link InFlightElement} for details on when an element no longer needs to be buffered.
	 *
	 * This method blocks the caller if there are currently no elements in the queue qualified to be emitted.
	 *
	 * @return The removed element.
	 */
	@Nullable
	public InFlightElement<OUT> removeNextEmittableElement() throws InterruptedException {
		synchronized (checkpointLock) {
			// if no emittable outputs, block until we find out there are elements that can be emitted
			// only either "add()" or "setOutputsForElement()" will wake us up
			if (!hasEmittableElements()) {
				checkpointLock.wait();
			}

			boolean seenIncompleteRecordElement = false;

			Iterator<Map.Entry<Long, InFlightElement<OUT>>> queueIterator = queue.entrySet().iterator();
			while (queueIterator.hasNext()) {
				InFlightElement<OUT> element = queueIterator.next().getValue();
				StreamElement streamElement = element.getElement();
				if (streamElement.isRecord()) {
					if (element.isOutputCollected()) {
						queueIterator.remove();

						// this wakes up blocks on "add()" if the queue was previously saturated
						checkpointLock.notifyAll();

						return element;
					} else {
						seenIncompleteRecordElement = true;
					}
				} else {
					if (!seenIncompleteRecordElement) {
						queueIterator.remove();

						// this wakes up blocks on "add()" if the queue was previously saturated
						checkpointLock.notifyAll();

						return element;
					} else {
						break;
					}
				}
			}
		}

		// shouldn't end up here, since we wake up from the block only when
		// there is emittable elements; this is just to silence the compiler
		return null;
	}

	public List<InFlightElement<OUT>> snapshotElements() {
		synchronized (checkpointLock) {
			List<InFlightElement<OUT>> snapshot = new ArrayList<>(queue.size());
			for (Map.Entry<Long, InFlightElement<OUT>> element : queue.entrySet()) {
				snapshot.add(element.getValue());
			}

			return snapshot;
		}
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
