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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.api.functions.MultiThreadedFlatMapFunction;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

public class StreamMultiThreadedFlatMapTest {

	/**
	 * Tests that state snapshotting and restoring works correctly, and that the internal operator state is coordinated
	 * with the MultiThreadedFlatMap function's user state.
	 *
	 * To test this, the test takes a total of 4 snapshots with a stateful MultiThreadedFlatMap function implementation,
	 * {@code ControllableStatefulMultiThreadedFlatMapper}. When restoring state, the original function implementation
	 * is replaced as a {@code RestoredStateValidationFlatMapper} that allows querying the restored user state,
	 * as well as which values were used to re-invoke the function.
	 *
	 * This test also implicitly tests that outputs from the operator is correct each time a snapshot is taken.
	 * Inline comments describe what each snapshot is expected to look like.
	 */
	@Test
	public void testStateSnapshotAndRestore() throws Exception {

		ControllableStatefulMultiThreadedFlatMapper function = new ControllableStatefulMultiThreadedFlatMapper();
		StreamMultiThreadedFlatMap<Integer, String> operator = new StreamMultiThreadedFlatMap<>(function, 3);
		OneInputStreamOperatorTestHarness<Integer, String> testHarness = new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.setTypeSerializerIn1(IntSerializer.INSTANCE);
		testHarness.setTypeSerializerOut(StringSerializer.INSTANCE);
		testHarness.setup();
		testHarness.open();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(1));
		testHarness.processElement(new StreamRecord<>(2));

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		// ---------- STATE #1 ----------
		// internal in-flight state: (record #1) -> (record #2)
		// function user state (seen values): (empty)
		// operator output: (empty)
		StreamStateHandle state1 = testHarness.snapshotLegacy(1L, System.currentTimeMillis());
		TestHarnessUtil.assertOutputEquals("", expectedOutput, testHarness.getOutput());

		function.triggerOutputForValue(2);
		expectedOutput.add(new StreamRecord<>("2"));

		// sleep a bit to make sure the function outputs
		Thread.sleep(500);

		// ---------- STATE #2 ----------
		// internal in-flight state: (record #1)
		// function user state (seen values): (record #2)
		// operator output: ("2")
		StreamStateHandle state2 = testHarness.snapshotLegacy(2L, System.currentTimeMillis());
		TestHarnessUtil.assertOutputEquals("", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new StreamRecord<>(3));
		testHarness.processWatermark(new Watermark(10));
		testHarness.processElement(new StreamRecord<>(4));

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		function.triggerOutputForValue(3);
		function.triggerOutputForValue(4);
		expectedOutput.add(new StreamRecord<>("3"));

		// sleep a bit to make sure the function outputs
		Thread.sleep(500);

		// ---------- STATE #3 ----------
		// internal in-flight state: (record #1) -> (watermark @ 10) -> (record #4, collected)
		// function user state (seen values): (record #2) -> (record #3) -> (record #4)
		// operator output: ("2") -> ("3")
		StreamStateHandle state3 = testHarness.snapshotLegacy(3L, System.currentTimeMillis());
		TestHarnessUtil.assertOutputEquals("", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new StreamRecord<>(5));
		testHarness.processElement(new StreamRecord<>(6));
		testHarness.processWatermark(new Watermark(20));

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		function.triggerOutputForValue(1);
		function.triggerOutputForValue(5);
		expectedOutput.add(new StreamRecord<>("1"));
		expectedOutput.add(new Watermark(10));
		expectedOutput.add(new StreamRecord<>("4"));
		expectedOutput.add(new StreamRecord<>("5"));

		// sleep a bit to make sure the function outputs
		Thread.sleep(500);

		// ---------- STATE #4 ----------
		// internal in-flight state: (record #6) -> (watermark @ 20)
		// function user state (seen values): (record #2) -> (record #3) -> (record #4) -> (record #1) -> (record #5)
		// operator output: ("2") -> ("3") -> ("1") -> (watermark @ 10) -> ("4") -> ("5")
		StreamStateHandle state4 = testHarness.snapshotLegacy(4L, System.currentTimeMillis());
		TestHarnessUtil.assertOutputEquals("", expectedOutput, testHarness.getOutput());


		// ----------------------------------------------------------------------------
		//  Test state restore
		// ----------------------------------------------------------------------------

		RestoredStateValidationFlatMapper restoredStateValidationFunction;

		HashSet<Integer> expectedSeenIntegers = new HashSet<>();
		HashSet<Integer> expectedReInvokedIntegers = new HashSet<>();
		List<InFlightElement<String>> expectedRestoredQueue = new LinkedList<>();

		// ---------- STATE #1 RESTORE ----------
		restoredStateValidationFunction= new RestoredStateValidationFlatMapper();
		operator = new StreamMultiThreadedFlatMap<>(restoredStateValidationFunction, 2);
		testHarness = new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.setTypeSerializerIn1(IntSerializer.INSTANCE);
		testHarness.setTypeSerializerOut(StringSerializer.INSTANCE);
		testHarness.setup();
		testHarness.restore(state1); // restore using state #1
		testHarness.open();

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		expectedReInvokedIntegers.add(1);
		expectedReInvokedIntegers.add(2);

		expectedRestoredQueue.add(new InFlightElement<String>(new StreamRecord<>(1)));
		expectedRestoredQueue.add(new InFlightElement<String>(new StreamRecord<>(2)));

		assertEquals(expectedSeenIntegers, restoredStateValidationFunction.getRestoredSeenIntegers());
		assertEquals(expectedReInvokedIntegers, restoredStateValidationFunction.getReInvokedIntegers());
		assertEquals(expectedRestoredQueue, operator.getQueue().snapshotElements());

		// ---------- STATE #2 RESTORE ----------
		restoredStateValidationFunction= new RestoredStateValidationFlatMapper();
		operator = new StreamMultiThreadedFlatMap<>(restoredStateValidationFunction, 3);
		testHarness = new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.setTypeSerializerIn1(IntSerializer.INSTANCE);
		testHarness.setTypeSerializerOut(StringSerializer.INSTANCE);
		testHarness.setup();
		testHarness.restore(state2); // restore using state #2
		testHarness.open();

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		expectedSeenIntegers.clear();
		expectedSeenIntegers.add(2);

		expectedReInvokedIntegers.clear();
		expectedReInvokedIntegers.add(1);

		expectedRestoredQueue.clear();
		expectedRestoredQueue.add(new InFlightElement<String>(new StreamRecord<>(1)));

		assertEquals(expectedSeenIntegers, restoredStateValidationFunction.getRestoredSeenIntegers());
		assertEquals(expectedReInvokedIntegers, restoredStateValidationFunction.getReInvokedIntegers());
		assertEquals(expectedRestoredQueue, operator.getQueue().snapshotElements());

		// ---------- STATE #3 RESTORE ----------
		restoredStateValidationFunction= new RestoredStateValidationFlatMapper();
		operator = new StreamMultiThreadedFlatMap<>(restoredStateValidationFunction, 3);
		testHarness = new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.setTypeSerializerIn1(IntSerializer.INSTANCE);
		testHarness.setTypeSerializerOut(StringSerializer.INSTANCE);
		testHarness.setup();
		testHarness.restore(state3); // restore using state #3
		testHarness.open();

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		expectedSeenIntegers.clear();
		expectedSeenIntegers.add(2);
		expectedSeenIntegers.add(3);
		expectedSeenIntegers.add(4);

		expectedReInvokedIntegers.clear();
		expectedReInvokedIntegers.add(1);

		expectedRestoredQueue.clear();
		expectedRestoredQueue.add(new InFlightElement<String>(new StreamRecord<>(1)));
		expectedRestoredQueue.add(new InFlightElement<String>(new Watermark(10)));
		InFlightElement<String> element4 = new InFlightElement<>(new StreamRecord<>(4));
		element4.setOutputCollection(Collections.singletonList("4"));
		expectedRestoredQueue.add(element4);

		assertEquals(expectedSeenIntegers, restoredStateValidationFunction.getRestoredSeenIntegers());
		assertEquals(expectedReInvokedIntegers, restoredStateValidationFunction.getReInvokedIntegers());
		assertEquals(expectedRestoredQueue, operator.getQueue().snapshotElements());

		// ---------- STATE #4 RESTORE ----------
		restoredStateValidationFunction= new RestoredStateValidationFlatMapper();
		operator = new StreamMultiThreadedFlatMap<>(restoredStateValidationFunction, 3);
		testHarness = new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.setTypeSerializerIn1(IntSerializer.INSTANCE);
		testHarness.setTypeSerializerOut(StringSerializer.INSTANCE);
		testHarness.setup();
		testHarness.restore(state4); // restore using state #4
		testHarness.open();

		// sleep a bit to make sure the function gets invoked
		Thread.sleep(500);

		expectedSeenIntegers.clear();
		expectedSeenIntegers.add(2);
		expectedSeenIntegers.add(3);
		expectedSeenIntegers.add(4);
		expectedSeenIntegers.add(1);
		expectedSeenIntegers.add(5);

		expectedReInvokedIntegers.clear();
		expectedReInvokedIntegers.add(6);

		expectedRestoredQueue.clear();
		expectedRestoredQueue.add(new InFlightElement<String>(new StreamRecord<>(6)));
		expectedRestoredQueue.add(new InFlightElement<String>(new Watermark(20)));

		assertEquals(expectedSeenIntegers, restoredStateValidationFunction.getRestoredSeenIntegers());
		assertEquals(expectedReInvokedIntegers, restoredStateValidationFunction.getReInvokedIntegers());
		assertEquals(expectedRestoredQueue, operator.getQueue().snapshotElements());
	}

	/**
	 * A stateful, controllable multi-threaded flat map function.
	 * Each input to {@code flatMap(...)} is blocked until {@code triggerOutputForValue(int)}
	 * is called with the exact same value.
	 */
	private class ControllableStatefulMultiThreadedFlatMapper
		implements MultiThreadedFlatMapFunction<Integer, String>, Checkpointed<HashSet<Integer>>{

		private static final long serialVersionUID = 1L;

		private ConcurrentHashMap<Integer, OneShotLatch> valueToLatch = new ConcurrentHashMap<>();

		private HashSet<Integer> seenIntegers = new HashSet<>();

		@Override
		public void flatMap(Integer value, Object checkpointLock, ExactlyOnceCollector<String> collector) throws Exception {
			if (!valueToLatch.containsKey(value)) {
				valueToLatch.put(value, new OneShotLatch());
			}

			valueToLatch.get(value).await();

			synchronized (checkpointLock) {
				seenIntegers.add(value);
				collector.collect(Collections.singletonList(String.valueOf(value)));
			}
		}

		@Override
		public HashSet<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return seenIntegers;
		}

		@Override
		public void restoreState(HashSet<Integer> state) throws Exception {
			this.seenIntegers = state;
		}

		public void triggerOutputForValue(int value) {
			valueToLatch.get(value).trigger();
		}
	}

	/**
	 * A multi-threaded flat map function used to query restored state,
	 * as well as what input values it was invoked with (useful to check
	 * which values the function was re-invoked with on restore).
	 */
	private class RestoredStateValidationFlatMapper
		implements MultiThreadedFlatMapFunction<Integer, String>, Checkpointed<HashSet<Integer>> {

		private static final long serialVersionUID = 1L;

		private HashSet<Integer> seenIntegers = new HashSet<>();

		private HashSet<Integer> reInvokedIntegers = new HashSet<>();

		@Override
		public void flatMap(Integer value, Object checkpointLock, ExactlyOnceCollector<String> collector) throws Exception {
			reInvokedIntegers.add(value);
		}

		@Override
		public HashSet<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return seenIntegers;
		}

		@Override
		public void restoreState(HashSet<Integer> state) throws Exception {
			this.seenIntegers = state;
		}

		public HashSet<Integer> getReInvokedIntegers() {
			return reInvokedIntegers;
		}

		public HashSet<Integer> getRestoredSeenIntegers() {
			return seenIntegers;
		}

	}

}
