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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InFlightElementsQueueTest {

	/**
	 * Tests that the queue removes and returns the correct next element qualified to be emitted.
	 * Note that this does not cover testing that removal blocks when there does not exist any
	 * qualified elements; this is tested in other tests below.
	 */
	@Test
	public void testRemoveNextEmittableElement() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(5, new Object());

		long index1 = queue.add(new StreamRecord<>(1));
		long index2 = queue.add(new StreamRecord<>(2));
		long index3 = queue.add(new Watermark(10));
		long index4 = queue.add(new StreamRecord<>(3));
		long index5 = queue.add(new LatencyMarker(20, 2, 5));

		InFlightElement<String> expectedNextEmittableElement;

		// deliberately set the output for the second record first; the return element should be the second record
		queue.setOutputsForElement(index2, Collections.singletonList("out2"));
		expectedNextEmittableElement = new InFlightElement<>(new StreamRecord<>(2));
		expectedNextEmittableElement.setOutputCollection(Collections.singletonList("out2"));
		assertEquals(expectedNextEmittableElement, queue.removeNextEmittableElement());

		queue.setOutputsForElement(index1, Collections.singletonList("out1"));
		expectedNextEmittableElement = new InFlightElement<>(new StreamRecord<>(1));
		expectedNextEmittableElement.setOutputCollection(Collections.singletonList("out1"));
		assertEquals(expectedNextEmittableElement, queue.removeNextEmittableElement());

		expectedNextEmittableElement = new InFlightElement<>(new Watermark(10));
		assertEquals(expectedNextEmittableElement, queue.removeNextEmittableElement());

		queue.setOutputsForElement(index4, Collections.singletonList("out3"));
		expectedNextEmittableElement = new InFlightElement<>(new StreamRecord<>(3));
		expectedNextEmittableElement.setOutputCollection(Collections.singletonList("out3"));
		assertEquals(expectedNextEmittableElement, queue.removeNextEmittableElement());

		expectedNextEmittableElement = new InFlightElement<>(new LatencyMarker(20, 2, 5));
		assertEquals(expectedNextEmittableElement, queue.removeNextEmittableElement());
	}

	/**
	 * Tests that if an output is set twice for the same record in the queue, should throw exception.
	 */
	@Test(expected = IllegalStateException.class)
	public void testResettingOutputThrowsIllegalStateException() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(1, new Object());

		long index1 = queue.add(new StreamRecord<>(1));
		queue.setOutputsForElement(index1, Collections.singletonList("out1"));

		// this should throw IllegalStateException
		queue.setOutputsForElement(index1, Collections.singletonList("out1"));
	}

	/**
	 * Tests that if a thread is blocked on adding elements to the queue, it is unblocked once
	 * an element is removed from the queue.
	 */
	@Test(timeout = 10000L)
	public void testRemoveWakesUpBlockOnAdd() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(2, new Object());

		// saturate the queue
		long index1 = queue.add(new StreamRecord<>(1));
		long index2 = queue.add(new StreamRecord<>(2));

		Thread addThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					queue.add(new StreamRecord<>(3));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		addThread.start();

		// the add should be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, addThread.getState());

		// let the queue have an element qualified to be removed
		queue.setOutputsForElement(index1, Collections.<String>emptyList());

		queue.removeNextEmittableElement();

		// the add thread should finish after space is allocated
		addThread.join();

		List<InFlightElement<String>> expectedQueueState = new ArrayList<>(2);
		expectedQueueState.add(new InFlightElement<String>(new StreamRecord<>(2)));
		expectedQueueState.add(new InFlightElement<String>(new StreamRecord<>(3)));
		assertEquals(expectedQueueState, queue.snapshotElements());
	}

	/**
	 * Tests that if a thread is blocked on removing an element from the queue because there is
	 * no elements in the queue qualified for removal, an add that results in qualified elements
	 * will unblock the removal.
	 */
	@Test(timeout = 10000L)
	public void testAddWakesUpBlockOnRemove() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(2, new Object());

		Thread removeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because the queue is empty
					queue.removeNextEmittableElement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		removeThread.start();

		// the remove should be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, removeThread.getState());

		// add a watermark to the queue (the add should mark the queue to have a removable element right away)
		queue.add(new Watermark(123));

		// the remove thread should finish right after the add
		removeThread.join();

		// since the watermark should be fetched by the remove thread, the queue should be empty
		assertEquals(Collections.emptyList(), queue.snapshotElements());
	}

	/**
	 * Tests that if a thread is blocked on removing an element from the queue because there is
	 * no elements in the queue qualified for removal, setting an output for a record that results in
	 * qualified elements will unblock the removal.
	 */
	@Test(timeout = 10000L)
	public void testSetOutputWakesUpBlockOnRemove() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(2, new Object());

		// saturate the queue, but without any elements qualified for removal
		long index1 = queue.add(new StreamRecord<>(1));
		queue.add(new Watermark(123));

		Thread removeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because there's no elements in the queue to be removed
					queue.removeNextEmittableElement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		removeThread.start();

		// the remove should be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, removeThread.getState());

		// let the queue have elements qualifed for removal
		queue.setOutputsForElement(index1, Collections.<String>emptyList());

		// the remove thread should finish right after the output setting
		removeThread.join();

		// only the watermark should remain, since the record element has been removed
		assertEquals(Collections.singletonList(new InFlightElement<>(new Watermark(123))), queue.snapshotElements());
	}

	/**
	 * Tests that if a thread is blocked on removing an element from the queue, while another thread is also blocked
	 * on adding an element because the queue is currently saturated, setting an output for a record that results in
	 * qualified elements will unblock the removal.
	 */
	@Test(timeout = 10000L)
	public void testSetOutputWakesUpBlockOnRemoveAndAdd() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(2, new Object());

		// saturate the queue, but without any elements qualified for removal
		long index1 = queue.add(new StreamRecord<>(1));
		queue.add(new Watermark(123));

		Thread addThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because the queue is currently saturated
					queue.add(new StreamRecord<>(2));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});

		Thread removeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because there's no elements in the queue to be removed
					queue.removeNextEmittableElement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});

		addThread.start();
		removeThread.start();

		// the add and remove threads should both be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, addThread.getState());
		assertEquals(Thread.State.WAITING, removeThread.getState());

		// let the queue have elements qualifed for removal
		queue.setOutputsForElement(index1, Collections.<String>emptyList());

		// the remove thread should finish right after the output setting;
		// the add thread should also finish right after, since there is now space in the queue after removal
		removeThread.join();
		addThread.join();

		// only the watermark and second added record should remain
		List<InFlightElement<String>> expectedQueueState = new ArrayList<>(2);
		expectedQueueState.add(new InFlightElement<String>(new Watermark(123)));
		expectedQueueState.add(new InFlightElement<String>(new StreamRecord<>(2)));
		assertEquals(expectedQueueState, queue.snapshotElements());
	}

	/**
	 * Tests that if a thread is blocked on removing an element from the queue, while another thread is also blocked
	 * on adding an element because the queue is currently saturated, setting an output for a record that still doesn't
	 * let the queue have qualified elements, the blocking on remove and add remains.
	 *
	 * The tested queue = (record #1) -> (watermark) -> (record #2).
	 * The output setting only sets the output for record #2, which still doesn't result in any qualified elements,
	 * because record #2 is after a buffered watermark.
	 */
	@Test
	public void testSetOutputDoesNotWakeUpBlockOnRemoveAndAdd() throws Exception {
		final InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(3, new Object());

		// saturate the queue, but without any elements qualified for removal
		long index1 = queue.add(new StreamRecord<>(1));
		queue.add(new Watermark(123));
		long index2 = queue.add(new StreamRecord<>(2));

		Thread addThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because the queue is currently saturated
					queue.add(new StreamRecord<>(3));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});

		Thread removeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block because there's no elements in the queue to be removed
					queue.removeNextEmittableElement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});

		addThread.start();
		removeThread.start();

		// the add and remove threads should both be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, addThread.getState());
		assertEquals(Thread.State.WAITING, removeThread.getState());

		// set the output only for the second record
		queue.setOutputsForElement(index2, Collections.<String>emptyList());

		// the add and remove threads should still be blocked
		Thread.sleep(2000);
		assertEquals(Thread.State.WAITING, addThread.getState());
		assertEquals(Thread.State.WAITING, removeThread.getState());
	}

	@Test
	public void testHasEmittableElements() throws Exception {
		InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(20, new Object());
		long index1 = queue.add(new StreamRecord<>("foo"));
		assertFalse(queue.hasEmittableElements());

		long index2 = queue.add(new StreamRecord<>("foo"));
		assertFalse(queue.hasEmittableElements());

		long index3 = queue.add(new Watermark(123));
		assertFalse(queue.hasEmittableElements());

		queue.setOutputsForElement(index1, Collections.<String>emptyList());
		assertTrue(queue.hasEmittableElements());

		InFlightElementsQueue queue2 = new InFlightElementsQueue(20, new Object());
		queue.add(new Watermark(456));
		assertTrue(queue.hasEmittableElements());
	}

}
