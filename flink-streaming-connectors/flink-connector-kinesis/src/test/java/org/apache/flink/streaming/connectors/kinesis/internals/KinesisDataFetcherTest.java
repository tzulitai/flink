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

package org.apache.flink.streaming.connectors.kinesis.internals;

import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class KinesisDataFetcherTest {

	// ------------------------------------------------------------------------
	//  Tests for correct construction of Kinesis Data Fetchers
	// ------------------------------------------------------------------------

	@Test
	public void testShouldNotBeRestoringFromCheckpointIfRestoredStateIsEmpty() {
		LinkedBlockingQueue<KinesisStreamShardState> pendingShardsQueueUnderTest = new LinkedBlockingQueue<>();
		TestableKinesisDataFetcher withEmptyRestoredStateMap =
			new TestableKinesisDataFetcher(
				Collections.singletonList("fakeStream"),
				new Properties(),
				10,
				2,
				new HashMap<KinesisStreamShard, String>(), // restored state map with no entries
				new AtomicReference<Throwable>(),
				pendingShardsQueueUnderTest,
				new LinkedList<KinesisStreamShardState>());

		assertTrue("Should not be flagged isRestoredFromCheckpoint if restored state map is empty",
			!withEmptyRestoredStateMap.isRestoredFromCheckpoint());
		assertTrue("No shards should be added to pendingShards queue if restored state map is empty",
			pendingShardsQueueUnderTest.size() == 0);
	}

	@Test
	public void testShouldNotBeRestoringFromCheckpointIfRestoredStateIsNull() {
		LinkedBlockingQueue<KinesisStreamShardState> pendingShardsQueueUnderTest = new LinkedBlockingQueue<>();
		TestableKinesisDataFetcher withEmptyRestoredStateMap =
			new TestableKinesisDataFetcher(
				Collections.singletonList("fakeStream"),
				new Properties(),
				10,
				2,
				new HashMap<KinesisStreamShard, String>(), // null restored state map
				new AtomicReference<Throwable>(),
				pendingShardsQueueUnderTest,
				new LinkedList<KinesisStreamShardState>());

		assertTrue("Should not be flagged isRestoredFromCheckpoint if restored state map is null",
			!withEmptyRestoredStateMap.isRestoredFromCheckpoint());
		assertTrue("No shards should be added to pendingShards queue if restored state map is null",
			pendingShardsQueueUnderTest.size() == 0);
	}

	@Test
	public void testPendingShardsAreSeededWhenRestoringFromCheckpointedState() {
		HashMap<KinesisStreamShard, String> restoredState = new HashMap<>();

		// set some random values for the restored state
		restoredState.put(
			new KinesisStreamShard("stream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(3))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(4))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredState.put(
			new KinesisStreamShard("stream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		LinkedBlockingQueue<KinesisStreamShardState> pendingShardsQueueUnderTest = new LinkedBlockingQueue<>();
		TestableKinesisDataFetcher fetcherUnderTest =
			new TestableKinesisDataFetcher(
				Collections.singletonList("fakeStream"),
				new Properties(),
				10,
				2,
				restoredState,
				new AtomicReference<Throwable>(),
				pendingShardsQueueUnderTest,
				subscribedShardsStateUnderTest);

		assertTrue("Should be flagged isRestoredFromCheckpoint if the fetcher has a non-empty restored state",
			fetcherUnderTest.isRestoredFromCheckpoint());
		assertTrue("Missing restored shards that should be seeded in the pendingShards queue",
			pendingShardsQueueUnderTest.size() == 8);

		// individually check that all shards of the restored state is seeded in the pending shards queue
		List<KinesisStreamShardState> pendingShardsList = Arrays.asList(
			pendingShardsQueueUnderTest.toArray(new KinesisStreamShardState[pendingShardsQueueUnderTest.size()]));
		for (Map.Entry<KinesisStreamShard, String> restoredStateEntry : restoredState.entrySet()) {
			assertTrue(
				"Found a restored shard that is missing in the pendingShards queue",
				pendingShardsList.contains(
					new KinesisStreamShardState(restoredStateEntry.getKey(), restoredStateEntry.getValue())));
		}
	}
}
