package org.apache.flink.contrib.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InFlightElementsQueueTest {

	@Test
	@SuppressWarnings("SynchronizeOnNonFinalField")
	public void testHasEmittableElements() {
		Object lock = new Object();

		InFlightElementsQueue<String> queue = new InFlightElementsQueue<>(lock);
		synchronized (lock) {
			long index1 = queue.add(new StreamRecord<>("foo"));
			assertFalse(queue.hasEmittableElements());

			long index2 = queue.add(new StreamRecord<>("foo"));
			assertFalse(queue.hasEmittableElements());

			long index3 = queue.add(new Watermark(123));
			assertFalse(queue.hasEmittableElements());

			queue.setOutputsForElement(index1, Collections.<String>emptyList());
			assertTrue(queue.hasEmittableElements());
		}

		InFlightElementsQueue queue2 = new InFlightElementsQueue(lock);
		synchronized (lock) {
			queue.add(new Watermark(456));
			assertTrue(queue.hasEmittableElements());
		}
	}

}
