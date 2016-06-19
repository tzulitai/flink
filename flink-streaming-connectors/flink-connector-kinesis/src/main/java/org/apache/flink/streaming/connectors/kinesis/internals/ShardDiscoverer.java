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

import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.IKinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This runnable is in charge of discovering new shards that a fetcher should subscribe to.
 * It is submitted to {@link KinesisDataFetcher#shardDiscovererAndSubscriberExecutor} and continuously runs until the
 * fetcher is closed. Whenever it discovers a new shard that should be subscribed to, the shard is added to the
 * {@link KinesisDataFetcher#pendingShards} queue with initial state, i.e. where in the new shard we should start
 * consuming from.
 */
public class ShardDiscoverer<T> implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ShardDiscoverer.class);

	/** This fetcher reference is used to add discovered shards to the pending shards queue */
	private final KinesisDataFetcher fetcherRef;

	/** Kinesis proxy to retrieve shard lists from Kinesis */
	private final IKinesisProxy kinesis;

	/**
	 * The last seen shard of each stream. Since new Kinesis shards are always created in ascending ids (regardless of
	 * whether the new shard was a result of a shard split or merge), this state can be used when calling
	 * {@link IKinesisProxy#getShardList(Map)} to ignore shards we have already discovered before.
	 */
	private final Map<String,String> streamToLastSeenShard;

	private final int totalNumberOfConsumerSubtasks;
	private final int indexOfThisConsumerSubtask;

	/**
	 * Create a new shard discoverer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 */
	public ShardDiscoverer(KinesisDataFetcher<T> fetcherRef) {
		this(fetcherRef, KinesisProxy.create(fetcherRef.getConsumerConfiguration()), new HashMap<String, String>());
	}

	/** This constructor is exposed for testing purposes */
	protected ShardDiscoverer(KinesisDataFetcher<T> fetcherRef,
							IKinesisProxy kinesis,
							Map<String,String> streamToLastSeenShard) {
		this.fetcherRef = checkNotNull(fetcherRef);
		this.kinesis = checkNotNull(kinesis);
		this.streamToLastSeenShard = checkNotNull(streamToLastSeenShard);

		this.totalNumberOfConsumerSubtasks = fetcherRef.getSubtaskRuntimeContext().getNumberOfParallelSubtasks();
		this.indexOfThisConsumerSubtask = fetcherRef.getSubtaskRuntimeContext().getIndexOfThisSubtask();

		// we initially map the last seen shard of each subscribed stream to null;
		// the correct values will be set later on in the constructor
		for (String stream : fetcherRef.getSubscribedStreams()) {
			this.streamToLastSeenShard.put(stream, null);
		}

		// if we are restoring from a checkpoint, the restored state should already be in the pending shards queue;
		// we iterate over the pending shards queue, and accordingly set the stream-to-last-seen-shard map
		if (fetcherRef.isRestoredFromCheckpoint()) {
			if (fetcherRef.getCurrentCountOfPendingShards() == 0) {
				throw new RuntimeException("Told to restore from checkpoint, but no shards found in discovered shards queue");
			}

			for (KinesisStreamShardState shardState : fetcherRef.cloneCurrentPendingShards()) {
				String stream = shardState.getShard().getStreamName();
				String shardId = shardState.getShard().getShardId();
				if (!this.streamToLastSeenShard.containsKey(stream)) {
					throw new RuntimeException(
						"pendingShards queue contains a shard belonging to a stream that we are not subscribing to");
				} else {
					String lastSeenShardIdOfStream = this.streamToLastSeenShard.get(stream);
					// the existing shards in the queue may not be in ascending id order,
					// so we must exhaustively find the largest shard id of each stream
					if (lastSeenShardIdOfStream == null) {
						// if not previously set, simply put as the last seen shard id
						this.streamToLastSeenShard.put(stream, shardId);
					} else if (KinesisStreamShard.compareShardIds(shardId, lastSeenShardIdOfStream) > 0) {
						// override if we have found a shard with a greater shard id for the stream
						this.streamToLastSeenShard.put(stream, shardId);
					}
				}
			}
		}

		// we always query for any new shards that may have been created while the Kinesis consumer was not running -
		// when we are starting fresh (not restoring from a checkpoint), this simply means all existing shards of streams
		// we are subscribing to are new shards; when we are restoring from checkpoint, any new shards due to Kinesis
		// resharding from the time of the checkpoint will be considered new shards.

		SentinelSequenceNumber sentinelSequenceNumber;
		if (!fetcherRef.isRestoredFromCheckpoint()) {
			// if starting fresh, each new shard should start from the user-configured position
			sentinelSequenceNumber =
				KinesisConfigUtil.getInitialPositionAsSentinelSequenceNumber(fetcherRef.getConsumerConfiguration());
		} else {
			// if restoring from checkpoint, the new shards due to Kinesis resharding should be read from earliest record
			sentinelSequenceNumber = SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
		}

		try {
			// query for new shards that we haven't seen yet up to this point
			discoverNewShardsAndSetInitialStateTo(sentinelSequenceNumber.toString());
		} catch (InterruptedException iex) {
			fetcherRef.stopWithError(iex);
			return;
		}

		boolean hasShards = false;
		StringBuilder streamsWithNoShardsFound = new StringBuilder();
		for (Map.Entry<String, String> streamToLastSeenShardEntry : streamToLastSeenShard.entrySet()) {
			if (streamToLastSeenShardEntry.getValue() != null) {
				hasShards = true;
			} else {
				streamsWithNoShardsFound.append(streamToLastSeenShardEntry.getKey()).append(", ");
			}
		}

		if (streamsWithNoShardsFound.length() != 0 && LOG.isWarnEnabled()) {
			LOG.warn("Subtask {} has failed to find any shards for the following subscribed streams: {}",
				indexOfThisConsumerSubtask, streamsWithNoShardsFound.toString());
		}

		if (!hasShards) {
			fetcherRef.stopWithError(new RuntimeException(
				"No shards can be found for all subscribed streams: " + fetcherRef.getSubscribedStreams()));
		}
	}

	@Override
	public void run() {
		try {
			while (isRunning()) {
				discoverNewShardsAndSetInitialStateTo(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.toString());
			}
		} catch (Throwable throwable) {
			fetcherRef.stopWithError(throwable);
		}
	}

	private boolean isRunning() {
		return !Thread.interrupted();
	}

	/**
	 * A utility function that does the following:
	 *
	 * 1. Find new shards for each stream that we haven't seen before
	 * 2. For each new shard, determine whether this consumer subtask should subscribe to them;
	 * 	  if yes, add the new shard to the discovered shards queue with a specified initial state (starting sequence num)
	 * 3. Update the stream-to-last-seen-shard map so that we won't get shards that we have already seen before
	 *    the next time this function is called
	 *
	 * @param initialState the initial state to assign to each new shard that this subtask should subscribe to
	 */
	private void discoverNewShardsAndSetInitialStateTo(String initialState) throws InterruptedException {
		GetShardListResult shardListResult = kinesis.getShardList(new HashMap<>(streamToLastSeenShard));
		if (shardListResult.hasRetrievedShards()) {
			Set<String> streamsWithNewShards = shardListResult.getStreamsWithRetrievedShards();

			for (String stream : streamsWithNewShards) {
				List<KinesisStreamShard> newShardsOfStream = shardListResult.getRetrievedShardListOfStream(stream);
				for (KinesisStreamShard newShard : newShardsOfStream) {
					if (isThisSubtaskShouldSubscribeTo(newShard)) {
						LOG.info("Discovered a new shard {} to subscribe to for subtask {}, adding to queue ...",
							newShard.toString(), indexOfThisConsumerSubtask);
						fetcherRef.addPendingShard(new KinesisStreamShardState(newShard, initialState));
					}
				}
				streamToLastSeenShard.put(stream, shardListResult.getLastSeenShardOfStream(stream).getShardId());
			}
		}
	}

	/**
	 * Utility function to determine whether a shard should be subscribed by this consumer subtask.
	 *
	 * @param shard the shard to determine
	 */
	private boolean isThisSubtaskShouldSubscribeTo(KinesisStreamShard shard) {
		return (Math.abs(shard.hashCode() % totalNumberOfConsumerSubtasks)) == indexOfThisConsumerSubtask;
	}
}
