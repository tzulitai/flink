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

import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordEntry;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A producer daemon that accepts records to publish to a single Kinesis stream,
 * and passes back the results of each record through a handler.
 *
 * The daemon manages two separate linked blocking queues, one for incoming records and one for outgoing results.
 * Incoming records are consumed batch by batch for producing. Each batch is asynchronously processed by tasks
 * submitted to a thread pool.
 */
public class StreamProducerDaemon {

	private static final Logger LOG = LoggerFactory.getLogger(StreamProducerDaemon.class);

	private static final int DEFAULT_PUT_RECORDS_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 4;

	/**
	 * Flag to indicate whether the daemon is currently shutting down.
	 * When set to true, no new records is allowed to be added to the queue.
	 *
	 * Note: there is already a shutdown mechanism in {@link FlinkKinesisProducer} that should block
	 * any new records after {@link FlinkKinesisProducer#close()} is called. We're implementing a shutdown
	 * mechanism here as well just in case.
	 */
	private volatile boolean shuttingDown = false;

	/**
	 * This flag will be set to false only when all records are flushed and all executor
	 * services have been shutdown after {@link StreamProducerDaemon#shutdown()} is called
	 */
	private volatile boolean running = true;

	/** Blocking queue for records awaiting to be published to the Kinesis stream */
	private BlockingQueue<KinesisRecordEntry> recordsToBePublished = new LinkedBlockingQueue<>();

	/** Blocking queue for results awaiting to be processed by the {@link StreamProducerDaemon#handler} */
	private BlockingQueue<KinesisRecordResult> resultsToBePassed = new LinkedBlockingQueue<>();

	/** Thread pool to execute long running loops that continuously process
	 * the blocking queues {@link StreamProducerDaemon#recordsToBePublished} and {@link StreamProducerDaemon#resultsToBePassed} */
	private static final AtomicInteger longRunningLoopsPoolNumber = new AtomicInteger(0);
	private final ExecutorService longRunningLoopsExecutor;

	/** Thread pool to execute short living background tasks that put records to the Kinesis stream */
	private static final AtomicInteger putRecordsPoolNumber = new AtomicInteger(0);
	private final ExecutorService putRecordsExecutor;

	/**
	 * Sentinels used to indicate the last element of blocking queues.
	 * Long running loops processing the blocking queues will not terminate until these sentinels are consumed.
	 */
	private static final KinesisRecordEntry ENTRY_SHUTDOWN_SENTINEL = generateShutdownSentinelForRecordQueue();
	private static final KinesisRecordResult RESULT_SHUTDOWN_SENTINEL = generateShutdownSentinelForResultQueue();

	public interface ResultHandler {
		void onResult(KinesisRecordResult result);
	}

	private final String taskName;

	private final KinesisProxy kinesisProxy;

	private final int maxNumberOfRecordsPerPut;

	private final String producingStream;

	private ResultHandler handler;

	public StreamProducerDaemon(String taskName,
								Properties props,
								String producingStream,
								ResultHandler handler) {
		this.taskName = Objects.requireNonNull(taskName);
		this.producingStream = Objects.requireNonNull(producingStream);
		this.kinesisProxy = new KinesisProxy(Objects.requireNonNull(props));
		this.maxNumberOfRecordsPerPut = Integer.valueOf(props.getProperty(
			"MAX_RECORDS_PER_PUT",
			"100"));

		this.handler = Objects.requireNonNull(handler);

		this.longRunningLoopsExecutor = Executors.newCachedThreadPool(
			new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("daemon-" + this.taskName + "-longRunningLoops-pool-" + longRunningLoopsPoolNumber.getAndIncrement() + "-thread-%d")
				.build());

		this.putRecordsExecutor = Executors.newFixedThreadPool(
			DEFAULT_PUT_RECORDS_THREAD_POOL_SIZE,
			new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("daemon-" + this.taskName + "-putRecords-pool-" + putRecordsPoolNumber.getAndIncrement() + "-thread-%d")
				.build());

		// long running loop to process incoming records
		longRunningLoopsExecutor.submit(new Runnable() {
			@Override
			public void run() {
				processRecordsToBePublishedLoop();
			}
		});

		// long running loop to process outgoing results
		longRunningLoopsExecutor.submit(new Runnable() {
			@Override
			public void run() {
				processResultsToBePassedLoop();
			}
		});
	}

	public void add(KinesisRecordEntry entry) {

		// forbid adding new records to publish when we have
		// been told to shutdown or the daemon has already terminated
		if (shuttingDown || !running) {
			return;
		}

		try {
			recordsToBePublished.put(entry);
		} catch (InterruptedException iex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Unexpected interruption", iex);
		}

	}

	public void shutdown() {
		shuttingDown = true;
		recordsToBePublished.add(ENTRY_SHUTDOWN_SENTINEL);
	}

	public void awaitTermination() {
		if (!shuttingDown) {
			throw new RuntimeException("A stream producer daemon that is not previously called to shutdown will never terminate.");
		}

		try {
			while (running) {
				Thread.sleep(500);
			}

			// finally, gracefully shutdown the executor for the long running loops
			longRunningLoopsExecutor.shutdown();

			// both long running loops should have already terminated, but we'll still await termination just in case
			longRunningLoopsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException iex) {
			throw new RuntimeException("Interrupted while waiting for stream producer daemon to terminate.");
		}
	}

	private void processRecordsToBePublishedLoop() {
		try {
			boolean shutdownSignalReceived = false;
			while (!shutdownSignalReceived) {
				List<KinesisRecordEntry> nextEntries = new ArrayList<>(maxNumberOfRecordsPerPut);

				// block until at least 1 record is present in the to-be-published queue
				KinesisRecordEntry nextOneEntry = recordsToBePublished.take();

				nextEntries.add(nextOneEntry);

				// fill up the set of next entries up to maximum number of records per put
				recordsToBePublished.drainTo(nextEntries, maxNumberOfRecordsPerPut - 1);

				if (nextEntries.contains(ENTRY_SHUTDOWN_SENTINEL)) {
					shutdownSignalReceived = true;
					// the last record should be the shutdown signal, otherwise something is wrong
					if (nextEntries.get(nextEntries.size()-1) != ENTRY_SHUTDOWN_SENTINEL) {
						throw new RuntimeException("Found entries after shutdown signal in producer daemon.");
					}

					// leave out the shutdown signal for publishing
					nextEntries = nextEntries.subList(0, nextEntries.size()-1);
				}

				putRecordsExecutor.submit(new BackgroundPutRecordsTask(nextEntries));
			}

		} catch (InterruptedException iex) {
			throw new RuntimeException("Interrupted while trying to consume a record for publishing", iex);
		} finally {
			// once we receive the shutdown signal and there are no more records to publish,
			// we wait for all current put records background tasks to finish before
			// also sending a shutdown signal to the result handling queue
			putRecordsExecutor.shutdown();
			try {
				putRecordsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException iex) {
				throw new RuntimeException("Interrupted while waiting for put records executor to terminate", iex);
			}
			resultsToBePassed.add(RESULT_SHUTDOWN_SENTINEL);
		}
	}

	private void processResultsToBePassedLoop() {
		try {
			KinesisRecordResult result;
			while ((result = resultsToBePassed.take()) != RESULT_SHUTDOWN_SENTINEL) {
				handler.onResult(result);
			}

			// only when all results are handled is the daemon defined to be not running anymore
			running = false;
		} catch (InterruptedException iex) {
			throw new RuntimeException("Interrupted while trying to consume a result to handle", iex);
		}
	}

	private static KinesisRecordEntry generateShutdownSentinelForRecordQueue() {
		String randomStrForSentinel = UUID.randomUUID().toString();
		return new KinesisRecordEntry(
			Long.MAX_VALUE, randomStrForSentinel, ByteBuffer.wrap(randomStrForSentinel.getBytes()), null, null);
	}

	private static KinesisRecordResult generateShutdownSentinelForResultQueue() {
		String randomStringForSentinel = UUID.randomUUID().toString();
		return new KinesisRecordResult(
			Long.MAX_VALUE,
			new PutRecordsResultEntry().withSequenceNumber(randomStringForSentinel).withShardId(randomStringForSentinel));
	}

	/**
	 * A task that tries to fully put all records to the Kinesis stream.
	 * Once a record is determined to be successfully put, its result will be added to the result queue immediately.
	 * When we get a partially-successful put records result (some failing records in the put batch),
	 * we retry up to a configured retry count only on the failed records.
	 * If there are still failing records after all attempts, we can only pass on the failing results.
	 */
	private class BackgroundPutRecordsTask implements Runnable {

		private final List<KinesisRecordEntry> remainingRecords;

		private static final int retryTimes = 3;

		public BackgroundPutRecordsTask(List<KinesisRecordEntry> recordsToPut) {
			this.remainingRecords = Objects.requireNonNull(recordsToPut);
		}

		@Override
		public void run() {

			int remainingRecordsCount = this.remainingRecords.size();

			if (remainingRecordsCount == 0) {
				// this might happen if the consumed batch of records from the record queue
				// contains only the ENTRY_SHUTDOWN_SENTINEL. If so, simply return.
				return;
			}

			PutRecordsResult putRecordsResultOnLastAttempt;
			List<PutRecordsResultEntry> listOfFailingRecordsOnLastAttempt = new ArrayList<>(remainingRecordsCount);
			int attemptCount = 0;

			while (attemptCount <= retryTimes && !remainingRecords.isEmpty()) {

				// refresh on every attempt
				remainingRecordsCount = this.remainingRecords.size();
				listOfFailingRecordsOnLastAttempt.clear();

				if (attemptCount >= 1) {
					LOG.warn("Last put records attempt had '{}' failing records" +
						", retrying for the '{}'th time", remainingRecordsCount, attemptCount);
				}

				putRecordsResultOnLastAttempt = kinesisProxy.putRecords(producingStream, remainingRecords);

				List<Integer> successfulRecordIndices = new ArrayList<>(remainingRecordsCount);

				for (int i=0; i<remainingRecordsCount; i++) {
					PutRecordsResultEntry result = putRecordsResultOnLastAttempt.getRecords().get(i);
					if (result.getSequenceNumber() != null) {
						// successful record
						try {
							resultsToBePassed.put(new KinesisRecordResult(this.remainingRecords.get(i).getId(), result));
						} catch (InterruptedException iex) {
							throw new RuntimeException("Interrupted while trying to put result to queue.", iex);
						}
						successfulRecordIndices.add(i);
					} else {
						// failing record
						listOfFailingRecordsOnLastAttempt.add(result);
					}
				}

				removeSuccessfulRecordsFromRemainingRecords(successfulRecordIndices);
				attemptCount++;
			}

			// pass on the failing results if there is still any remaining
			if (!remainingRecords.isEmpty()) {
				// if we don't have matching record counts on the maintained lists, there's something wrong
				if (listOfFailingRecordsOnLastAttempt.size() != remainingRecordsCount) {
					throw new RuntimeException("The number of remaining records after all attempts doesn't " +
						"match the number of failing records on last attempt");
				}

				for (int i=0; i<listOfFailingRecordsOnLastAttempt.size(); i++) {
					try {
						resultsToBePassed.put(new KinesisRecordResult(
							this.remainingRecords.get(i).getId(),
							listOfFailingRecordsOnLastAttempt.get(i)));
					} catch (InterruptedException iex) {
						throw new RuntimeException("Interrupted while trying to put result to queue.", iex);
					}
				}
			}
		}

		private void removeSuccessfulRecordsFromRemainingRecords(List<Integer> indicesOfSuccessfulRecords) {
			// need to sort the indices into descending order first
			// to avoid messing up the ordering of the original records
			Collections.sort(indicesOfSuccessfulRecords, Collections.reverseOrder());
			for (int index : indicesOfSuccessfulRecords) {
				this.remainingRecords.remove(index);
			}
		}

	}
}
