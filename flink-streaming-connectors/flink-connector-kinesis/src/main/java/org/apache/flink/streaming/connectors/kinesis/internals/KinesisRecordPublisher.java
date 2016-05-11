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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordEntry;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordFailedException;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordResult;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Kinesis Record Publisher that manages multiple {@link StreamProducerDaemon}s, each responsible for publishing records
 * to a single Kinesis stream (a daemon will only be instantiated when an incoming record has a stream not seen before).
 * The publisher returns a future when a record is added, passes the record to a corresponding daemon, and completes the
 * future when the daemon reports the result of the record.
 */
public class KinesisRecordPublisher {

	/** Configuration for the Flink Kinesis Producer */
	private final Properties configProps;

	/** The name of the sink task that this publisher was instantiated */
	private final String taskName;

	/** Map with daemon per stream */
	private final Map<String, StreamProducerDaemon> producerDaemonsByStream = new HashMap<>();

	/** Atomic Long to keep track of the number of records added to this publisher */
	private final AtomicLong recordNumber = new AtomicLong(0);

	/** Put record result futures (mapped by record id) that are waiting to be completed */
	private final Map<Long, SettableFuture<?>> futureToBeCompleted = new ConcurrentHashMap<>();

	/** Thread pool to complete result futures */
	private static final AtomicInteger callbackCompletionPoolNumber = new AtomicInteger(0);
	private final ExecutorService callbackCompletionExecutor;

	/** Handler that is given to each {@link StreamProducerDaemon} spawned */
	public class ResultHandler implements StreamProducerDaemon.ResultHandler {
		@Override
		public void onResult(final KinesisRecordResult result) {
			callbackCompletionExecutor.submit(new Runnable() {
				@Override
				public void run() {
					SettableFuture<KinesisRecordResult> future = getFuture(result.getId());
					if (result.isSuccessful()) {
						future.set(result);
					} else {
						future.setException(new KinesisRecordFailedException(result));
					}
				}
			});
		}

		private <T> SettableFuture<T> getFuture(Long recordId) {
			@SuppressWarnings("unchecked")
			SettableFuture<T> future = (SettableFuture<T>) futureToBeCompleted.remove(recordId);
			if (future == null) {
				// this should not happen ...
				throw new RuntimeException("Future for record id " + recordId + " not found");
			}
			return future;
		}
	}

	public KinesisRecordPublisher(String region, String accessKey, String secretKey, String taskName) {
		this.configProps = new Properties();
		this.configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, region);
		this.configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, accessKey);
		this.configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, secretKey);
		this.taskName = taskName;

		this.callbackCompletionExecutor = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors() * 4,
			new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("publisher-" + this.taskName + "-callback-pool-" + callbackCompletionPoolNumber.getAndIncrement() + "-thread-%d")
				.build());
	}

	public ListenableFuture<KinesisRecordResult> addRecordToPublish(String stream,
																	ByteBuffer serialized,
																	String partition,
																	String explicitHashkey) {
		if (!producerDaemonsByStream.containsKey(stream)) {
			// create a new daemon for a previously unseen stream
			producerDaemonsByStream.put(stream, new StreamProducerDaemon(taskName, configProps, stream, new ResultHandler()));
		}

		long recordId = this.recordNumber.getAndIncrement();
		SettableFuture<KinesisRecordResult> listenableFuture = SettableFuture.create();
		futureToBeCompleted.put(recordId, listenableFuture);

		producerDaemonsByStream.get(stream).add(new KinesisRecordEntry(
			recordId, stream, serialized, partition, explicitHashkey));

		return listenableFuture;
	}

	public long getOutstandingRecordsCount() {
		return futureToBeCompleted.size();
	}

	public void shutdown() {
		// command all daemons to start shutdown, causing them to shutdown all threads after flushing all remaining records
		for (StreamProducerDaemon daemon : producerDaemonsByStream.values()) {
			daemon.shutdown();
		}
	}

	public void destroy() {
		// first wait for all daemons to finish shutdown
		for (StreamProducerDaemon daemon : producerDaemonsByStream.values()) {
			daemon.awaitTermination();
		}

		if (futureToBeCompleted.size() != 0) {
			// if there are still futures to be completed, there's something wrong
			throw new RuntimeException("There are still " + futureToBeCompleted.size() +
				" records not fully processed after all daemons have been shutdown");
		}

		callbackCompletionExecutor.shutdown();
		try {
			// there should be no more running completion tasks submitted to the executor, but we'll still wait just in case
			callbackCompletionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException iex) {
			throw new RuntimeException("Interrupted while waiting for callback completion thread pool to shutdown", iex);
		}
	}

}
