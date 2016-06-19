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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A KinesisDataFetcher is responsible for fetching data from multiple Kinesis shards. Each parallel subtask instantiates
 * and runs a single fetcher throughout the subtask's lifetime. The fetcher runs several threads to accomplish
 * the following:
 * <ul>
 *     <li>1. continously poll Kinesis to discover shards that the subtask should subscribe to. The subscribed subset
 *     		  of shards, including future new shards, is non-overlapping across subtasks (no two subtasks will be
 *     		  subscribed to the same shard) and determinate across subtask restores (the subtask will always subscribe
 *     		  to the same subset of shards even after restoring)</li>
 *     <li>2. decide where in each discovered shard should the fetcher start subscribing to</li>
 *     <li>3. subscribe to shards by creating a single thread for each shard</li>
 * </ul>
 *
 * <p>The fetcher manages two states: 1) pending shards for subscription, and 2) last processed sequence numbers of
 * each subscribed shard. All operations on the states in multiple threads should only be done using the handler methods
 * provided in this class.
 */
public class KinesisDataFetcher<T> {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisDataFetcher.class);

	// ------------------------------------------------------------------------
	//  Consumer-wide settings
	// ------------------------------------------------------------------------

	/** Configuration properties for the Flink Kinesis Consumer */
	private final Properties configProps;

	/** The list of Kinesis streams that the consumer is subscribing to */
	private final List<String> streams;

	/**
	 * The deserialization schema we will be using to convert Kinesis records to Flink objects.
	 * Note that since this might not be thread-safe, multiple threads in the fetcher using this must
	 * clone a copy using {@link KinesisDataFetcher#getClonedDeserializationSchema()}.
	 */
	private final KinesisDeserializationSchema<T> deserializationSchema;

	// ------------------------------------------------------------------------
	//  Subtask-specific settings
	// ------------------------------------------------------------------------

	/** Runtime context of the subtask that this fetcher was created in */
	private final RuntimeContext runtimeContext;

	// ------------------------------------------------------------------------
	//  Executor services to run created threads
	// ------------------------------------------------------------------------

	/** Executor service to run the {@link ShardDiscoverer} and {@link ShardSubscriber} */
	private final ExecutorService shardDiscovererAndSubscriberExecutor;

	/** Executor service to run {@link ShardConsumer}s to consumer Kinesis shards */
	private final ExecutorService shardConsumersExecutor;

	// ------------------------------------------------------------------------
	//  Managed state, accessed and updated across multiple threads
	// ------------------------------------------------------------------------

	/**
	 * Blocking queue for newly discovered shards, with their states, that this fetcher should consume.
	 * The {@link ShardDiscoverer} will add shards with initial position as state to this queue as shards are discovered,
	 * while the {@link ShardSubscriber} polls this queue to start subscribing to the new discovered shards.
	 */
	private final BlockingQueue<KinesisStreamShardState> pendingShards;

	/**
	 * The shards, along with their last processed sequence numbers, that this fetcher is subscribed to. The shard
	 * subscriber will add to this list as it polls pending shards. Shard consumer threads update the last processed
	 * sequence number of subscribed shards as they fetch and process records.
	 *
	 * <p>Note that since multiple threads will be performing operations on this list, all operations must be wrapped in
	 * synchronized blocks on the {@link KinesisDataFetcher#checkpointLock} lock. For this purpose, all threads must use
	 * the following thread-safe methods this class provides to operate on this list:
	 * <ul>
	 *     <li>{@link KinesisDataFetcher#addAndStartConsumingNewSubscribedShard(KinesisStreamShardState)}</li>
	 *     <li>{@link KinesisDataFetcher#updateState(int, String)}</li>
	 *     <li>{@link KinesisDataFetcher#emitRecordAndUpdateState(Object, int, String)}</li>
	 * </ul>
	 */
	private final List<KinesisStreamShardState> subscribedShardsState;

	private final SourceFunction.SourceContext<T> sourceContext;

	/** Checkpoint lock, also used to synchronize operations on subscribedShardsState */
	private final Object checkpointLock;

	/** This flag is set to true if the fetcher is provided a non-null and non-empty restored state */
	private final boolean isRestoredFromCheckpoint;

	/** Reference to the first error thrown by any of the created threads */
	private final AtomicReference<Throwable> error;

	private volatile boolean running = true;

	/**
	 * Creates a Kinesis Data Fetcher.
	 *
	 * @param streams the streams to subscribe to
	 * @param sourceContext context of the source function
	 * @param runtimeContext this subtask's runtime context
	 * @param configProps the consumer configuration properties
	 * @param restoredState state of subcribed shards that the fetcher should restore to
	 * @param deserializationSchema deserialization schema
	 */
	public KinesisDataFetcher(List<String> streams,
							SourceFunction.SourceContext<T> sourceContext,
							RuntimeContext runtimeContext,
							Properties configProps,
							Map<KinesisStreamShard, String> restoredState,
							KinesisDeserializationSchema<T> deserializationSchema) {
		this(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,configProps,
			restoredState,
			deserializationSchema,
			new AtomicReference<Throwable>(),
			new LinkedBlockingQueue<KinesisStreamShardState>(),
			new LinkedList<KinesisStreamShardState>());
	}

	/** This constructor is exposed for testing purposes */
	protected KinesisDataFetcher(List<String> streams,
								SourceFunction.SourceContext<T> sourceContext,
								Object checkpointLock,
								RuntimeContext runtimeContext,
								Properties configProps,
								Map<KinesisStreamShard, String> restoredState,
								KinesisDeserializationSchema<T> deserializationSchema,
								AtomicReference<Throwable> error,
								LinkedBlockingQueue<KinesisStreamShardState> pendingShardsQueue,
								LinkedList<KinesisStreamShardState> subscribedShardsState) {
		this.streams = checkNotNull(streams);
		this.configProps = checkNotNull(configProps);
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.runtimeContext = checkNotNull(runtimeContext);
		this.deserializationSchema = checkNotNull(deserializationSchema);

		this.error = error;
		this.pendingShards = pendingShardsQueue;
		this.subscribedShardsState = subscribedShardsState;

		this.shardDiscovererAndSubscriberExecutor =
			createShardDiscovererAndSubscriberThreadPool(runtimeContext.getTaskNameWithSubtasks());
		this.shardConsumersExecutor =
			createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());

		this.isRestoredFromCheckpoint = (restoredState != null && restoredState.entrySet().size() != 0);

		// if there is state to restore from last checkpoint, we seed them as initially discovered shards
		if (isRestoredFromCheckpoint) {
			seedPendingShardsWithRestoredState(restoredState, this.pendingShards);
		}
	}

	/**
	 * Starts the fetcher. After starting the fetcher, it can only
	 * be stopped by calling {@link KinesisDataFetcher#shutdownFetcher()}.
	 *
	 * @throws Exception the first error or exception thrown by the fetcher or any of the threads created by the fetcher.
	 */
	public void runFetcher() throws Exception {

		if (LOG.isInfoEnabled()) {
			LOG.info("Subtask {} is starting the shard discoverer ...", runtimeContext.getIndexOfThisSubtask());
		}
		shardDiscovererAndSubscriberExecutor.submit(new ShardDiscoverer<>(this));

		// after this point we will start fetching data from Kinesis and update internal state,
		// so we check that we are running for the last time before continuing
		if (!running) {
			return;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Subtask {} is starting the shard subscriber ...", runtimeContext.getIndexOfThisSubtask());
		}
		shardDiscovererAndSubscriberExecutor.submit(new ShardSubscriber<>(this));

		while (running) {
			// once either shutdownFetcher() or stopWithError()
			// is called, we will escape this loop
		}

		// make sure all threads and executor services have been terminated before leaving
		awaitTermination();

		// any error thrown in either the fetcher thread, shard discoverer / assigner thread,
		// or shard consumer threads will be thrown to the main thread
		Throwable throwable = this.error.get();
		if (throwable != null) {
			if (throwable instanceof Exception) {
				throw (Exception) throwable;
			} else if (throwable instanceof Error) {
				throw (Error) throwable;
			} else {
				throw new Exception(throwable);
			}
		}
	}

	/**
	 * Creates a snapshot of the current last processed sequence numbers of each subscribed shard.
	 *
	 * @return state snapshot
	 */
	public HashMap<KinesisStreamShard, String> snapshotState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<KinesisStreamShard, String> stateSnapshot = new HashMap<>();
		for (KinesisStreamShardState shardWithState : subscribedShardsState) {
			stateSnapshot.put(shardWithState.getShard(), shardWithState.getLastProcessedSequenceNum());
		}
		return stateSnapshot;
	}

	/**
	 * Starts breaking down the fetcher. Must be called to stop loop in {@link KinesisDataFetcher#runFetcher()}.
	 * Once called, the breakdown procedure will be executed and all threads will be interrupted.
	 */
	public void shutdownFetcher() {
		executeBreakdownProcedure();
	}

	/** After calling {@link KinesisDataFetcher#shutdownFetcher()}, this can be called to await the fetcher breakdown */
	public void awaitTermination() throws InterruptedException {
		while(!shardDiscovererAndSubscriberExecutor.isTerminated() && !shardConsumersExecutor.isTerminated()) {
			Thread.sleep(10);
		}
	}

	/** Called by created threads to pass on errors. Only the first thrown error is set.
	 * Once set, the breakdown procedure will be executed and all threads will be interrupted. */
	protected void stopWithError(Throwable throwable) {
		if (this.error.compareAndSet(null, throwable)) {
			executeBreakdownProcedure();
		}
	}

	// ------------------------------------------------------------------------
	//  Operations for the pendingShards queue
	// ------------------------------------------------------------------------

	/**
	 * Adds a shard to the pending shards queue.
	 * This method is called by {@link ShardDiscoverer} and blocks until the shard is succesfully added.
	 *
	 * @param shardWithState the shard to add to the queue, along with its starting position as initial state
	 */
	protected void addPendingShard(KinesisStreamShardState shardWithState) throws InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Adding shard {} to the discovered shards queue of subtask {} ...",
				shardWithState.toString(), runtimeContext.getIndexOfThisSubtask());
		}

		pendingShards.put(shardWithState);
	}

	/**
	 * Polls a shard from the pending shards queue.
	 *
	 * This method is called by {@link ShardSubscriber} and blocks until a pending shard is found.
	 *
	 * @return the polled shard
	 */
	protected KinesisStreamShardState pollPendingShard() throws InterruptedException {
		KinesisStreamShardState shard = pendingShards.take();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Got shard {} from the discovered shards queue of subtask {}",
				shard.toString(), runtimeContext.getIndexOfThisSubtask());
		}

		return shard;
	}

	/**
	 * Get an array of the current content of the pending shards queue.
	 */
	protected KinesisStreamShardState[] cloneCurrentPendingShards() {
		return pendingShards.toArray(new KinesisStreamShardState[pendingShards.size()]);
	}

	/**
	 * Get the current amount of the pending shards in the queue.
	 */
	protected int getCurrentCountOfPendingShards() {
		return pendingShards.size();
	}

	// ------------------------------------------------------------------------
	//  Functions for threads to access information about the consumer
	// ------------------------------------------------------------------------

	protected RuntimeContext getSubtaskRuntimeContext() {
		return runtimeContext;
	}

	protected Properties getConsumerConfiguration() {
		return configProps;
	}

	protected KinesisDeserializationSchema<T> getClonedDeserializationSchema() {
		try {
			return InstantiationUtil.clone(deserializationSchema, runtimeContext.getUserCodeClassLoader());
		} catch (IOException | ClassNotFoundException ex) {
			// this really shouldn't happen; simply wrap it around a runtime exception
			throw new RuntimeException(ex);
		}
	}

	protected List<String> getSubscribedStreams() {
		return streams;
	}

	protected boolean isRestoredFromCheckpoint() {
		return isRestoredFromCheckpoint;
	}

	// ------------------------------------------------------------------------
	//  Thread-safe operations for record emitting and shard state updating
	//  that assure atomicity with respect to the checkpoint lock
	// ------------------------------------------------------------------------

	/**
	 * Atomic operation to collect a record and update state to the sequence number of the record.
	 * This method is called by {@link ShardConsumer}s. The shard state index is assigned to consumers in
	 * {@link KinesisDataFetcher#addAndStartConsumingNewSubscribedShard(KinesisStreamShardState)} when the consumer is started.
	 *
	 * @param record the record to collect
	 * @param shardStateIndex index of the shard to update in subcribedShardsState
	 * @param lastSequenceNumber the last sequence number value to update
	 */
	protected void emitRecordAndUpdateState(T record, int shardStateIndex, String lastSequenceNumber) {
		synchronized (checkpointLock) {
			sourceContext.collect(record);
			updateState(shardStateIndex, lastSequenceNumber);
		}
	}

	/**
	 * Update the shard to last processed sequence number state.
	 * This method is called by {@link ShardConsumer}s. The shard state index is assigned to consumers in
	 * {@link KinesisDataFetcher#addAndStartConsumingNewSubscribedShard(KinesisStreamShardState)} when the consumer is started.
	 *
	 * @param shardStateIndex index of the shard to update in subscribedShardsState
	 * @param lastSequenceNumber the last sequence number value to update
	 */
	protected void updateState(int shardStateIndex, String lastSequenceNumber) {
		synchronized (checkpointLock) {
			subscribedShardsState.get(shardStateIndex).setLastProcessedSequenceNum(lastSequenceNumber);
		}
	}

	/**
	 * Adds a new subscribed shard to the shard to last processed sequence number state, and
	 * creates a new {@link ShardConsumer} to start consuming the shard.
	 *
	 * @param newSubscribedShard the new shard that this fetcher is to be subscribed to
	 */
	protected void addAndStartConsumingNewSubscribedShard(KinesisStreamShardState newSubscribedShard)
		throws IOException, ClassNotFoundException {

		int stateIndexOfNewConsumer;

		synchronized (checkpointLock) {
			subscribedShardsState.add(newSubscribedShard);
			stateIndexOfNewConsumer = subscribedShardsState.size()-1;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Subtask {} has subscribed to a new shard {}, and will start consuming " +
					"the shard from sequence number {} with ShardConsumer {}",
				runtimeContext.getIndexOfThisSubtask(), newSubscribedShard.getShard().toString(),
				newSubscribedShard.getLastProcessedSequenceNum(), stateIndexOfNewConsumer);
		}

		shardConsumersExecutor.submit(
			new ShardConsumer<>(
				this,
				stateIndexOfNewConsumer,
				newSubscribedShard.getShard(),
				newSubscribedShard.getLastProcessedSequenceNum()));
	}


	// ------------------------------------------------------------------------
	//  Miscellaneous utility functions
	// ------------------------------------------------------------------------

	private static ExecutorService createShardDiscovererAndSubscriberThreadPool(final String subtaskName) {
		// thread pool size fixed to 2 for the shard discoverer and subscriber
		return Executors.newFixedThreadPool(2, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				final AtomicLong threadCount = new AtomicLong(0);
				Thread thread = new Thread(runnable);
				thread.setName("shardDiscovererAndSubscriber-" + subtaskName + "-thread-" + threadCount.getAndIncrement());
				thread.setDaemon(true);
				return thread;
			}
		});
	}

	private static ExecutorService createShardConsumersThreadPool(final String subtaskName) {
		return Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				final AtomicLong threadCount = new AtomicLong(0);
				Thread thread = new Thread(runnable);
				thread.setName("shardConsumers-" + subtaskName + "-thread-" + threadCount.getAndIncrement());
				thread.setDaemon(true);
				return thread;
			}
		});
	}

	private static void seedPendingShardsWithRestoredState(Map<KinesisStreamShard, String> restoredState,
														BlockingQueue<KinesisStreamShardState> pendingShardsQueue) {
		try {
			for (Map.Entry<KinesisStreamShard, String> restoreSequenceNum : restoredState.entrySet()) {
				pendingShardsQueue.put(new KinesisStreamShardState(restoreSequenceNum.getKey(), restoreSequenceNum.getValue()));
			}
		} catch (InterruptedException iex) {
			throw new RuntimeException(iex);
		}
	}

	private void executeBreakdownProcedure() {
		this.running = false;

		if (shardDiscovererAndSubscriberExecutor != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Shutting down the shard discoverer and assigner threads of subtask {}",
					runtimeContext.getIndexOfThisSubtask());
			}
			shardDiscovererAndSubscriberExecutor.shutdownNow();
		}

		if (shardConsumersExecutor != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Shutting down the shard consumer threads of subtask {}",
					runtimeContext.getIndexOfThisSubtask());
			}
			shardConsumersExecutor.shutdownNow();
		}
	}
}
