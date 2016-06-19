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

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.IKinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread that does the actual data pulling from AWS Kinesis shards. Each thread is in charge of one Kinesis shard only.
 */
public class ShardConsumer<T> implements Runnable {

	private final KinesisDeserializationSchema<T> deserializer;

	private final IKinesisProxy kinesis;

	private final int subscribedShardStateIndex;

	private final KinesisDataFetcher fetcherRef;

	private final KinesisStreamShard subscribedShard;

	private final int maxNumberOfRecordsPerFetch;

	private String lastSequenceNum;

	/**
	 * Creates a shard consumer.
	 *
	 * @param fetcherRef reference to the owning fetcher
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 */
	public ShardConsumer(KinesisDataFetcher<T> fetcherRef,
						Integer subscribedShardStateIndex,
						KinesisStreamShard subscribedShard,
						String lastSequenceNum) {
		this(fetcherRef,
			subscribedShardStateIndex,
			subscribedShard,
			lastSequenceNum,
			KinesisProxy.create(fetcherRef.getConsumerConfiguration()));
	}

	/** This constructor is exposed for testing purposes */
	protected ShardConsumer(KinesisDataFetcher<T> fetcherRef,
							Integer subscribedShardStateIndex,
							KinesisStreamShard subscribedShard,
							String lastSequenceNum,
							IKinesisProxy kinesis) {
		this.fetcherRef = checkNotNull(fetcherRef);
		this.subscribedShardStateIndex = checkNotNull(subscribedShardStateIndex);
		this.subscribedShard = checkNotNull(subscribedShard);
		this.lastSequenceNum = checkNotNull(lastSequenceNum);

		this.deserializer = fetcherRef.getClonedDeserializationSchema();

		Properties consumerConfig = fetcherRef.getConsumerConfiguration();
		this.kinesis = kinesis;
		this.maxNumberOfRecordsPerFetch = Integer.valueOf(consumerConfig.getProperty(
			KinesisConfigConstants.CONFIG_SHARD_GETRECORDS_MAX,
			Integer.toString(KinesisConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {

		String nextShardItr;
		try {
			if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.toString())) {
				// if the shard is already closed, there will be no latest next record to get for this shard
				if (subscribedShard.isClosed()) {
					nextShardItr = null;
				} else {
					nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.LATEST.toString(), null);
				}
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.toString())) {
				nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.TRIM_HORIZON.toString(), null);
			} else if (lastSequenceNum.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.toString())) {
				nextShardItr = null;
			} else {
				nextShardItr = kinesis.getShardIterator(subscribedShard, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), lastSequenceNum);
			}

			while(isRunning()) {
				if (nextShardItr == null) {
					lastSequenceNum = SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.toString();

					fetcherRef.updateState(subscribedShardStateIndex, lastSequenceNum);

					// we can close this consumer thread once we've reached the end of the subscribed shard
					break;
				} else {
					GetRecordsResult getRecordsResult = kinesis.getRecords(nextShardItr, maxNumberOfRecordsPerFetch);

					List<Record> fetchedRecords = getRecordsResult.getRecords();

					// each of the Kinesis records may be aggregated, so we must deaggregate them before proceeding
					fetchedRecords = deaggregateRecords(fetchedRecords, subscribedShard.getStartingHashKey(), subscribedShard.getEndingHashKey());

					for (Record record : fetchedRecords) {
						ByteBuffer recordData = record.getData();

						byte[] dataBytes = new byte[recordData.remaining()];
						recordData.get(dataBytes);

						byte[] keyBytes = record.getPartitionKey().getBytes();

						final T value = deserializer.deserialize(keyBytes, dataBytes, subscribedShard.getStreamName(),
							record.getSequenceNumber());

						fetcherRef.emitRecordAndUpdateState(value, subscribedShardStateIndex, record.getSequenceNumber());

						lastSequenceNum = record.getSequenceNumber();
					}

					nextShardItr = getRecordsResult.getNextShardIterator();
				}
			}
		} catch (Throwable t) {
			fetcherRef.stopWithError(t);
		}
	}

	/**
	 * The loop in run() checks this before fetching next batch of records. Since this runnable will be executed
	 * by the ExecutorService {@link KinesisDataFetcher#shardConsumersExecutor}, the only way to close down this thread
	 * would be by calling shutdownNow() on {@link KinesisDataFetcher#shardConsumersExecutor} and let the executor service
	 * interrupt all currently running {@link ShardConsumer}s.
	 */
	private boolean isRunning() {
		return !Thread.interrupted();
	}

	@SuppressWarnings("unchecked")
	protected static List<Record> deaggregateRecords(List<Record> records, String startingHashKey, String endingHashKey) {
		return (List<Record>) (List<?>) UserRecord.deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
	}
}
