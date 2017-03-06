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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedRestoring;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of all Flink Kafka Consumer data sources.
 * This implements the common behavior across all Kafka versions.
 * 
 * <p>The Kafka version specific behavior is defined mainly in the specific subclasses of the
 * {@link AbstractFetcher}.
 * 
 * @param <T> The type of records produced by this data source
 */
public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T> implements 
		CheckpointListener,
		ResultTypeQueryable<T>,
		CheckpointedFunction,
		CheckpointedRestoring<HashMap<KafkaTopicPartition, Long>> {

	private static final long serialVersionUID = -6272159445203409112L;

	protected static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);
	
	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	/** The default interval to execute partition discovery, in milliseconds */
	public static final long DEFAULT_PARTITION_DISCOVERY_INTERVAL_MILLIS = 10000L;

	/** Boolean configuration key to disable metrics tracking **/
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/** Configuration key to define the consumer's partition discovery interval, in milliseconds */
	public static final String KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "flink.partition-discovery.interval-millis";

	// ------------------------------------------------------------------------
	//  configuration state, set on the client relevant for all subtasks
	// ------------------------------------------------------------------------

	/** Describes whether we are discovering partitions for fixed topics or a topic pattern */
	private final KafkaTopicsDescriptor topicsDescriptor;

	/** The schema to convert between Kafka's byte messages, and Flink's objects */
	protected final KeyedDeserializationSchema<T> deserializer;

	/** The set of topic partitions that the source will read, with their initial offsets to start reading from */
	private Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets;
	
	/** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics.
	 * The assigner is kept in serialized form, to deserialize it into multiple copies */
	private SerializedValue<AssignerWithPeriodicWatermarks<T>> periodicWatermarkAssigner;
	
	/** Optional timestamp extractor / watermark generator that will be run per Kafka partition,
	 * to exploit per-partition timestamp characteristics. 
	 * The assigner is kept in serialized form, to deserialize it into multiple copies */
	private SerializedValue<AssignerWithPunctuatedWatermarks<T>> punctuatedWatermarkAssigner;

	private transient ListState<Tuple2<KafkaTopicPartition, Long>> offsetsStateForCheckpoint;

	/** User configured value for discovery interval, in milliseconds */
	private final long discoveryIntervalMillis;

	/** The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}) */
	protected StartupMode startupMode = StartupMode.GROUP_OFFSETS;

	// ------------------------------------------------------------------------
	//  runtime state (used individually by each parallel subtask) 
	// ------------------------------------------------------------------------
	
	/** Data for pending but uncommitted offsets */
	private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

	/** The fetcher implements the connections to the Kafka brokers */
	private transient volatile AbstractFetcher<T, ?> kafkaFetcher;

	/** The partition discoverer, used to find new partitions */
	private transient volatile AbstractPartitionDiscoverer partitionDiscoverer;

	/** The offsets to restore to, if the consumer restores state from a checkpoint */
	private transient volatile HashMap<KafkaTopicPartition, Long> restoredState;

	/** Reference to the thread that invoked run(). This is used to interrupt partition discovery on shutdown. */
	private transient volatile Thread runThread;

	/** Flag indicating whether the consumer is still running */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	/**
	 * Base constructor.
	 *
	 * @param topics fixed list of topics to subscribe to (null, if using topic pattern)
	 * @param topicPattern the topic pattern to subscribe to (null, if using fixed topics)
	 * @param deserializer The deserializer to turn raw byte messages into Java/Scala objects.
	 */
	public FlinkKafkaConsumerBase(
			List<String> topics,
			Pattern topicPattern,
			KeyedDeserializationSchema<T> deserializer,
			long discoveryIntervalMillis) {
		this.topicsDescriptor = new KafkaTopicsDescriptor(topics, topicPattern);
		this.deserializer = checkNotNull(deserializer, "valueDeserializer");

		checkArgument(discoveryIntervalMillis >= 0);
		this.discoveryIntervalMillis = discoveryIntervalMillis;
	}

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------
	
	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
	 * The watermark extractor will run per Kafka partition, watermarks will be merged across partitions
	 * in the same way as in the Flink runtime, when streams are merged.
	 * 
	 * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
	 * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
	 * per Kafka partition, they will not be strictly ascending in the resulting Flink DataStream, if the
	 * parallel source subtask reads more that one partition.
	 * 
	 * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per Kafka
	 * partition, allows users to let them exploit the per-partition characteristics.
	 * 
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 * 
	 * @param assigner The timestamp assigner / watermark generator to use.
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> assigner) {
		checkNotNull(assigner);
		
		if (this.periodicWatermarkAssigner != null) {
			throw new IllegalStateException("A periodic watermark emitter has already been set.");
		}
		try {
			ClosureCleaner.clean(assigner, true);
			this.punctuatedWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
	 * The watermark extractor will run per Kafka partition, watermarks will be merged across partitions
	 * in the same way as in the Flink runtime, when streams are merged.
	 *
	 * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
	 * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
	 * per Kafka partition, they will not be strictly ascending in the resulting Flink DataStream, if the
	 * parallel source subtask reads more that one partition.
	 *
	 * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per Kafka
	 * partition, allows users to let them exploit the per-partition characteristics.
	 *
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 *
	 * @param assigner The timestamp assigner / watermark generator to use.
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> assigner) {
		checkNotNull(assigner);
		
		if (this.punctuatedWatermarkAssigner != null) {
			throw new IllegalStateException("A punctuated watermark emitter has already been set.");
		}
		try {
			ClosureCleaner.clean(assigner, true);
			this.periodicWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	/**
	 * Specifies the consumer to start reading from the earliest offset for all partitions.
	 * This ignores any committed group offsets in Zookeeper / Kafka brokers.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromEarliest() {
		this.startupMode = StartupMode.EARLIEST;
		return this;
	}

	/**
	 * Specifies the consumer to start reading from the latest offset for all partitions.
	 * This ignores any committed group offsets in Zookeeper / Kafka brokers.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromLatest() {
		this.startupMode = StartupMode.LATEST;
		return this;
	}

	/**
	 * Specifies the consumer to start reading from any committed group offsets found
	 * in Zookeeper / Kafka brokers. The "group.id" property must be set in the configuration
	 * properties. If no offset can be found for a partition, the behaviour in "auto.offset.reset"
	 * set in the configuration properties will be used for the partition.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromGroupOffsets() {
		this.startupMode = StartupMode.GROUP_OFFSETS;
		return this;
	}

	// ------------------------------------------------------------------------
	//  Work methods
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration configuration) throws Exception {
		// create the partition discoverer
		this.partitionDiscoverer = createPartitionDiscoverer(
				topicsDescriptor,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks());
		this.partitionDiscoverer.initializeConnections();

		subscribedPartitionsToStartOffsets = new HashMap<>();

		if (restoredState != null) {
			for (Map.Entry<KafkaTopicPartition, Long> restoredStateEntry : restoredState.entrySet()) {
				subscribedPartitionsToStartOffsets.put(restoredStateEntry.getKey(), restoredStateEntry.getValue());

				// we also need to update what partitions the partition discoverer has seen already
				// TODO   Currently, after a restore, partition discovery will not work correctly
				// TODO   because the discoverer can not fully recover all globally seen records
				partitionDiscoverer.checkAndSetDiscoveredPartition(restoredStateEntry.getKey());
			}

			LOG.info("Consumer subtask {} will start reading {} partitions with offsets in restored state: {}",
				getRuntimeContext().getIndexOfThisSubtask(), subscribedPartitionsToStartOffsets.size(), subscribedPartitionsToStartOffsets);
		} else {
			// use the partition discoverer to fetch the initial seed partitions
			for (KafkaTopicPartition seedPartition : partitionDiscoverer.discoverPartitions()) {
				subscribedPartitionsToStartOffsets.put(seedPartition, startupMode.getStateSentinel());
			}

			if (subscribedPartitionsToStartOffsets.size() != 0) {
				switch (startupMode) {
					case EARLIEST:
						LOG.info("Consumer subtask {} will start reading the following {} partitions from the earliest offsets: {}",
							getRuntimeContext().getIndexOfThisSubtask(),
							subscribedPartitionsToStartOffsets.size(),
							subscribedPartitionsToStartOffsets.keySet());
						break;
					case LATEST:
						LOG.info("Consumer subtask {} will start reading the following {} partitions from the latest offsets: {}",
							getRuntimeContext().getIndexOfThisSubtask(),
							subscribedPartitionsToStartOffsets.size(),
							subscribedPartitionsToStartOffsets.keySet());
						break;
					default:
					case GROUP_OFFSETS:
						LOG.info("Consumer subtask {} will start reading the following {} partitions from the committed group offsets in Kafka: {}",
							getRuntimeContext().getIndexOfThisSubtask(),
							subscribedPartitionsToStartOffsets.size(),
							subscribedPartitionsToStartOffsets.keySet());
				}
			}
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (subscribedPartitionsToStartOffsets == null) {
			throw new Exception("The partitions were not set for the consumer");
		}

		this.runThread = Thread.currentThread();

		// mark the subtask as temporarily idle if there are no initial seed partitions;
		// once this subtask discovers some partitions and starts collecting records, the subtask's
		// state will automatically be triggered back to be active.
		if (subscribedPartitionsToStartOffsets.isEmpty()) {
			sourceContext.markAsTemporarilyIdle();
		}

		// create the fetcher that will communicate with the Kafka brokers
		final AbstractFetcher<T, ?> fetcher = createFetcher(
				sourceContext, subscribedPartitionsToStartOffsets,
				periodicWatermarkAssigner, punctuatedWatermarkAssigner,
				(StreamingRuntimeContext) getRuntimeContext());

		// publish the reference, for snapshot-, commit-, and cancel calls
		// IMPORTANT: We can only do that now, because only now will calls to
		//            the fetchers 'snapshotCurrentState()' method return at least
		//            the restored offsets
		this.kafkaFetcher = fetcher;

		if (!running) {
			return;
		}

		final AtomicReference<Exception> fetcherErrorRef = new AtomicReference<>();
		Thread fetcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// run the fetcher' main work method
					fetcher.runFetchLoop();
				} catch (Exception e) {
					fetcherErrorRef.set(e);
				} finally {
					// calling cancel will also let the partition discovery loop escape
					cancel();
				}
			}
		});
		fetcherThread.start();

		// --------------------- partition discovery loop ---------------------

		List<KafkaTopicPartition> discoveredPartitions;
		while (running) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Consumer subtask {} is trying to discover new partitions ...");
			}

			discoveredPartitions = partitionDiscoverer.discoverPartitions();
			if (!discoveredPartitions.isEmpty()) {
				fetcher.addDiscoveredPartitions(discoveredPartitions);
			}

			// eagerly check to see if we're still running, to not waste any time sleeping
			if (running && discoveryIntervalMillis != 0) {
				try {
					Thread.sleep(discoveryIntervalMillis);
				} catch (InterruptedException iex) {
					// we may wake up early if the consumer is canceled and
					// we're not running anymore; simply escape from the loop
				}
			}
		}

		// --------------------------------------------------------------------

		fetcherThread.join();

		// rethrow any fetcher errors
		Exception fetcherError = fetcherErrorRef.get();
		if (fetcherError != null) {
			throw new RuntimeException(fetcherError);
		}
	}

	@Override
	public void cancel() {
		// set ourselves as not running
		running = false;

		// the discovery loop in run() may be sleeping during an interval;
		// interrupt the run thread to shutdown faster.
		if (runThread != null) {
			runThread.interrupt();
		}

		// abort the fetcher, if there is one
		if (kafkaFetcher != null) {
			kafkaFetcher.cancel();
		}

		if (partitionDiscoverer != null) {
			try {
				partitionDiscoverer.closeConnections();
			} catch (Exception e) {
				throw new RuntimeException("Error while closing partition discoverer.", e);
			}
		}

		// there will be an interrupt() call to the main thread anyways
	}

	@Override
	public void close() throws Exception {
		// pretty much the same logic as cancelling
		try {
			cancel();
		} finally {
			super.close();
		}
	}
	
	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		OperatorStateStore stateStore = context.getOperatorStateStore();
		offsetsStateForCheckpoint = stateStore.getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

		if (context.isRestored()) {
			if (restoredState == null) {
				restoredState = new HashMap<>();
				for (Tuple2<KafkaTopicPartition, Long> kafkaOffset : offsetsStateForCheckpoint.get()) {
					restoredState.put(kafkaOffset.f0, kafkaOffset.f1);
				}

				LOG.info("Setting restore state in the FlinkKafkaConsumer.");
				if (LOG.isDebugEnabled()) {
					LOG.debug("Using the following offsets: {}", restoredState);
				}
			} else if (restoredState.isEmpty()) {
				restoredState = null;
			}
		} else {
			LOG.info("No restore state for FlinkKafkaConsumer.");
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
		} else {

			offsetsStateForCheckpoint.clear();

			final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
			if (fetcher == null) {
				// the fetcher has not yet been initialized, which means we need to return the
				// originally restored offsets or the assigned partitions
				for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition : subscribedPartitionsToStartOffsets.entrySet()) {
					offsetsStateForCheckpoint.add(Tuple2.of(subscribedPartition.getKey(), subscribedPartition.getValue()));
				}

				// the map cannot be asynchronously updated, because only one checkpoint call can happen
				// on this function at a time: either snapshotState() or notifyCheckpointComplete()
				pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
			} else {
				HashMap<KafkaTopicPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

				// the map cannot be asynchronously updated, because only one checkpoint call can happen
				// on this function at a time: either snapshotState() or notifyCheckpointComplete()
				pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);

				for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry : currentOffsets.entrySet()) {
					offsetsStateForCheckpoint.add(
							Tuple2.of(kafkaTopicPartitionLongEntry.getKey(), kafkaTopicPartitionLongEntry.getValue()));
				}
			}

			// truncate the map of pending offsets to commit, to prevent infinite growth
			while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
				pendingOffsetsToCommit.remove(0);
			}
		}
	}

	@Override
	public void restoreState(HashMap<KafkaTopicPartition, Long> restoredOffsets) {
		LOG.info("{} (taskIdx={}) restoring offsets from an older version.",
			getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());

		restoredState = restoredOffsets;

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) restored offsets from an older Flink version: {}",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), restoredState);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}

		final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
		if (fetcher == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}
		
		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing offsets to Kafka/ZooKeeper for checkpoint " + checkpointId);
		}

		try {
			final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
			if (posInMap == -1) {
				LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
				return;
			}

			@SuppressWarnings("unchecked")
			HashMap<KafkaTopicPartition, Long> offsets =
					(HashMap<KafkaTopicPartition, Long>) pendingOffsetsToCommit.remove(posInMap);

			// remove older checkpoints in map
			for (int i = 0; i < posInMap; i++) {
				pendingOffsetsToCommit.remove(0);
			}

			if (offsets == null || offsets.size() == 0) {
				LOG.debug("Checkpoint state was empty.");
				return;
			}
			fetcher.commitInternalOffsetsToKafka(offsets);
		}
		catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if we are no longer running
		}
	}

	// ------------------------------------------------------------------------
	//  Kafka Consumer specific methods
	// ------------------------------------------------------------------------
	
	/**
	 * Creates the fetcher that connect to the Kafka brokers, pulls data, deserialized the
	 * data, and emits it into the data streams.
	 * 
	 * @param sourceContext The source context to emit data to.
	 * @param subscribedPartitionsToStartOffsets The set of partitions that this subtask should handle, with their start offsets.
	 * @param watermarksPeriodic Optional, a serialized timestamp extractor / periodic watermark generator.
	 * @param watermarksPunctuated Optional, a serialized timestamp extractor / punctuated watermark generator.
	 * @param runtimeContext The task's runtime context.
	 * 
	 * @return The instantiated fetcher
	 * 
	 * @throws Exception The method should forward exceptions
	 */
	protected abstract AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext) throws Exception;

	/**
	 * Creates the partition discoverer that is used to find new partitions for this subtask.
	 *
	 * @param topicsDescriptor Descriptor that describes whether we are discovering partitions for fixed topics or a topic pattern.
	 * @param indexOfThisSubtask The index of this consumer subtask.
	 * @param numParallelSubtasks The total number of parallel consumer subtasks.
	 *
	 * @return The instantiated partition discoverer
	 */
	protected abstract AbstractPartitionDiscoverer createPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks);

	// ------------------------------------------------------------------------
	//  ResultTypeQueryable methods 
	// ------------------------------------------------------------------------
	
	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  Test utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	void setSubscribedPartitions(List<KafkaTopicPartition> allSubscribedPartitions) {
		checkNotNull(allSubscribedPartitions);
		this.subscribedPartitionsToStartOffsets = new HashMap<>();
		for (KafkaTopicPartition partition : allSubscribedPartitions) {
			this.subscribedPartitionsToStartOffsets.put(partition, null);
		}
	}

	@VisibleForTesting
	Map<KafkaTopicPartition, Long> getSubscribedPartitionsToStartOffsets() {
		return subscribedPartitionsToStartOffsets;
	}

	@VisibleForTesting
	HashMap<KafkaTopicPartition, Long> getRestoredState() {
		return restoredState;
	}
}
