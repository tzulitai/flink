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

import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Shard Subscriber simply polls for new shards found by the Shard Discoverer. When a new shard is found,
 * it updates the subscribed shards state, and creates a new thread to start consuming it.
 */
public class ShardSubscriber<T> implements Runnable {

	private final KinesisDataFetcher<T> fetcherRef;

	public ShardSubscriber(KinesisDataFetcher<T> fetcherRef) {
		this.fetcherRef = checkNotNull(fetcherRef);
	}

	@Override
	public void run() {
		KinesisStreamShardState newShardWithState;
		try {
			while(isRunning()) {
				newShardWithState = fetcherRef.pollPendingShard();
				fetcherRef.addAndStartConsumingNewSubscribedShard(newShardWithState);
			}
		} catch (Throwable throwable) {
			fetcherRef.stopWithError(throwable);
		}
	}

	private boolean isRunning() {
		return !Thread.interrupted();
	}
}
