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
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class ShardDiscovererTest {

	@Test
	public void testIfNoShardsAreFoundShouldThrowException() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		AtomicReference<Throwable> errorUnderTest = new AtomicReference<>();
		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				null, // not restoring from checkpoint
				errorUnderTest,
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour(),
			streamToLastSeenShardStateUnderTest);

		Throwable thrownError = errorUnderTest.get();
		assertTrue(thrownError != null);
		assertTrue(thrownError instanceof RuntimeException);
		assertTrue(thrownError.getMessage().contains("No shards can be found for all subscribed streams: "));

		for (Map.Entry<String, String> streamToLastSeenShardEntry : streamToLastSeenShardStateUnderTest.entrySet()) {
			assertTrue("All shard", streamToLastSeenShardEntry.getValue() == null);
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNotRestoringFromCheckpoint() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");
		fakeStreams.add("fakeStream3");
		fakeStreams.add("fakeStream4");

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				null, // not restoring from checkpoint
				new AtomicReference<Throwable>(),
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		Random rand = new Random();
		for (String fakeStream : fakeStreams) {
			streamToShardCount.put(fakeStream, rand.nextInt(5)+1);
		}

		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();
		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount),
			streamToLastSeenShardStateUnderTest);

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = streamToLastSeenShardStateUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String,String> streamToLastSeenShard : streamToLastSeenShardStateUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpoint() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				restoredStateUnderTest,
				new AtomicReference<Throwable>(),
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3); // fakeStream1 will still have 3 shards after restore
		streamToShardCount.put("fakeStream2", 2); // fakeStream2 will still have 2 shards after restore

		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();
		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount),
			streamToLastSeenShardStateUnderTest);

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = streamToLastSeenShardStateUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String,String> streamToLastSeenShard : streamToLastSeenShardStateUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpoint() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				restoredStateUnderTest,
				new AtomicReference<Throwable>(),
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3+1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2+3); // fakeStream2 had 2 shards before & 3 new shard after restore

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();
		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount),
			streamToLastSeenShardStateUnderTest);

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = streamToLastSeenShardStateUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String,String> streamToLastSeenShard : streamToLastSeenShardStateUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpointAndSomeStreamsDoNotExist() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");
		fakeStreams.add("fakeStream3"); // fakeStream3 will not have any shards
		fakeStreams.add("fakeStream4"); // fakeStream4 will not have any shards

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());


		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				restoredStateUnderTest,
				new AtomicReference<Throwable>(),
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3); // fakeStream1 has fixed 3 shards
		streamToShardCount.put("fakeStream2", 2); // fakeStream2 has fixed 2 shards
		streamToShardCount.put("fakeStream3", 0); // no shards can be found for fakeStream3
		streamToShardCount.put("fakeStream4", 0); // no shards can be found for fakeStream4

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();
		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount),
			streamToLastSeenShardStateUnderTest);

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = streamToLastSeenShardStateUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream1").equals(
			KinesisShardIdGenerator.generateFromShardOrder(2)));
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream2").equals(
			KinesisShardIdGenerator.generateFromShardOrder(1)));
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream3") == null);
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream4") == null);
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhennNewShardsFoundSinceRestoredCheckpointAndSomeStreamsDoNotExist() {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");
		fakeStreams.add("fakeStream3"); // fakeStream3 will not have any shards
		fakeStreams.add("fakeStream4"); // fakeStream4 will not have any shards

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());


		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				restoredStateUnderTest,
				new AtomicReference<Throwable>(),
				new LinkedBlockingQueue<KinesisStreamShardState>(),
				new LinkedList<KinesisStreamShardState>());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3+1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2+3); // fakeStream2 had 2 shards before & 2 new shard after restore
		streamToShardCount.put("fakeStream3", 0); // no shards can be found for fakeStream3
		streamToShardCount.put("fakeStream4", 0); // no shards can be found for fakeStream4

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
		Map<String,String> streamToLastSeenShardStateUnderTest = new HashMap<>();
		new ShardDiscoverer<>(
			fetcher,
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount),
			streamToLastSeenShardStateUnderTest);

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = streamToLastSeenShardStateUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream1").equals(
			KinesisShardIdGenerator.generateFromShardOrder(3)));
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream2").equals(
			KinesisShardIdGenerator.generateFromShardOrder(4)));
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream3") == null);
		assertTrue(streamToLastSeenShardStateUnderTest.get("fakeStream4") == null);
	}

}
