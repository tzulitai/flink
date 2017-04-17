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
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitModes;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

	/**
	 * User-set flag determining whether or not to commit on checkpoints.
	 * Note: this flag does not represent the final offset commit mode.
	 */
	private boolean enableCommitOnCheckpoints = true;

	/**
	 * The offset commit mode for the consumer.
	 * The value of this can only be determined in {@link FlinkKafkaConsumerBase#open(Configuration)} since it depends
	 * on whether or not checkpointing is enabled for the job.
	 */
	private OffsetCommitMode offsetCommitMode;

	/** User configured value for discovery interval, in milliseconds */
	private final long discoveryIntervalMillis;

	/** The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}) */
	private StartupMode startupMode = StartupMode.GROUP_OFFSETS;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS} */
	protected Map<KafkaTopicPartition, Long> specificStartupOffsets;

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

	/** Reference to the thread that invoked run(). This is used to interrupt partition discovery on shutdown */
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
	 * Specifies whether or not the consumer should commit offsets back to Kafka on checkpoints.
	 *
	 * This setting will only have effect if checkpointing is enabled for the job.
	 * If checkpointing isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
	 * property settings will be
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setCommitOffsetsOnCheckpoints(boolean commitOnCheckpoints) {
		this.enableCommitOnCheckpoints = commitOnCheckpoints;
		return this;
	}

	/**
	 * Specifies the consumer to start reading from the earliest offset for all partitions.
	 * This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
	 *
	 * This method does not effect where partitions are read from when the consumer is restored
	 * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
	 * savepoint, only the offsets in the restored state will be used.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromEarliest() {
		this.startupMode = StartupMode.EARLIEST;
		this.specificStartupOffsets = null;
		return this;
	}

	/**
	 * Specifies the consumer to start reading from the latest offset for all partitions.
	 * This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
	 *
	 * This method does not effect where partitions are read from when the consumer is restored
	 * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
	 * savepoint, only the offsets in the restored state will be used.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromLatest() {
		this.startupMode = StartupMode.LATEST;
		this.specificStartupOffsets = null;
		return this;
	}

	/**
	 * Specifies the consumer to start reading from any committed group offsets found
	 * in Zookeeper / Kafka brokers. The "group.id" property must be set in the configuration
	 * properties. If no offset can be found for a partition, the behaviour in "auto.offset.reset"
	 * set in the configuration properties will be used for the partition.
	 *
	 * This method does not effect where partitions are read from when the consumer is restored
	 * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
	 * savepoint, only the offsets in the restored state will be used.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromGroupOffsets() {
		this.startupMode = StartupMode.GROUP_OFFSETS;
		this.specificStartupOffsets = null;
		return this;
	}

	/**
	 * Specifies the consumer to start reading partitions from specific offsets, set independently for each partition.
	 * The specified offset should be the offset of the next record that will be read from partitions.
	 * This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
	 *
	 * If the provided map of offsets contains entries whose {@link KafkaTopicPartition} is not subscribed by the
	 * consumer, the entry will be ignored. If the consumer subscribes to a partition that does not exist in the provided
	 * map of offsets, the consumer will fallback to the default group offset behaviour (see
	 * {@link FlinkKafkaConsumerBase#setStartFromGroupOffsets()}) for that particular partition.
	 *
	 * If the specified offset for a partition is invalid, or the behaviour for that partition is defaulted to group
	 * offsets but still no group offset could be found for it, then the "auto.offset.reset" behaviour set in the
	 * configuration properties will be used for the partition
	 *
	 * This method does not effect where partitions are read from when the consumer is restored
	 * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
	 * savepoint, only the offsets in the restored state will be used.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkKafkaConsumerBase<T> setStartFromSpecificOffsets(Map<KafkaTopicPartition, Long> specificStartupOffsets) {
		this.startupMode = StartupMode.SPECIFIC_OFFSETS;
		this.specificStartupOffsets = checkNotNull(specificStartupOffsets);
		return this;
	}

	// ------------------------------------------------------------------------
	//  Work methods
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration configuration) throws Exception {
		// determine the offset commit mode
		offsetCommitMode = OffsetCommitModes.fromConfiguration(
				getIsAutoCommitEnabled(),
				enableCommitOnCheckpoints,
				((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled());

		// create the partition discoverer
		this.partitionDiscoverer = createPartitionDiscoverer(
				topicsDescriptor,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks());
		this.partitionDiscoverer.open();

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
				if (startupMode != StartupMode.SPECIFIC_OFFSETS) {
					subscribedPartitionsToStartOffsets.put(seedPartition, startupMode.getStateSentinel());
				} else {
					if (specificStartupOffsets == null) {
						throw new IllegalArgumentException(
							"Startup mode for the consumer set to " + StartupMode.SPECIFIC_OFFSETS +
								", but no specific offsets were specified");
					}

					Long specificOffset = specificStartupOffsets.get(seedPartition);
					if (specificOffset != null) {
						// since the specified offsets represent the next record to read, we subtract
						// it by one so that the initial state of the consumer will be correct
						subscribedPartitionsToStartOffsets.put(seedPartition, specificOffset - 1);
					} else {
						subscribedPartitionsToStartOffsets.put(seedPartition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
					}
				}
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
					case SPECIFIC_OFFSETS:
						LOG.info("Consumer subtask {} will start reading the following {} partitions from the specified startup offsets {}: {}",
							getRuntimeContext().getIndexOfThisSubtask(),
							subscribedPartitionsToStartOffsets.size(),
							specificStartupOffsets,
							subscribedPartitionsToStartOffsets.keySet());

						List<KafkaTopicPartition> partitionsDefaultedToGroupOffsets = new ArrayList<>(subscribedPartitionsToStartOffsets.size());
						for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition : subscribedPartitionsToStartOffsets.entrySet()) {
							if (subscribedPartition.getValue() == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
								partitionsDefaultedToGroupOffsets.add(subscribedPartition.getKey());
							}
						}

						if (partitionsDefaultedToGroupOffsets.size() > 0) {
							LOG.warn("Consumer subtask {} cannot find offsets for the following {} partitions in the specified startup offsets: {}" +
									"; their startup offsets will be defaulted to their committed group offsets in Kafka.",
								getRuntimeContext().getIndexOfThisSubtask(),
								partitionsDefaultedToGroupOffsets.size(),
								partitionsDefaultedToGroupOffsets);
						}
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
		// status will automatically be triggered back to be active.
		if (subscribedPartitionsToStartOffsets.isEmpty()) {
			sourceContext.markAsTemporarilyIdle();
		}

		// create the fetcher that will communicate with the Kafka brokers
		final AbstractFetcher<T, ?> fetcher = createFetcher(
				sourceContext,
				subscribedPartitionsToStartOffsets,
				periodicWatermarkAssigner,
				punctuatedWatermarkAssigner,
				(StreamingRuntimeContext) getRuntimeContext(),
				offsetCommitMode);

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

			try {
				discoveredPartitions = partitionDiscoverer.discoverPartitions();
			} catch (AbstractPartitionDiscoverer.ClosedException e) {
				break;
			}

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
				partitionDiscoverer.close();
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

				if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
					// the map cannot be asynchronously updated, because only one checkpoint call can happen
					// on this function at a time: either snapshotState() or notifyCheckpointComplete()
					pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
				}
			} else {
				HashMap<KafkaTopicPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

				if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
					// the map cannot be asynchronously updated, because only one checkpoint call can happen
					// on this function at a time: either snapshotState() or notifyCheckpointComplete()
					pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
				}

				for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry : currentOffsets.entrySet()) {
					offsetsStateForCheckpoint.add(
							Tuple2.of(kafkaTopicPartitionLongEntry.getKey(), kafkaTopicPartitionLongEntry.getValue()));
				}
			}

			if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
				// truncate the map of pending offsets to commit, to prevent infinite growth
				while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
					pendingOffsetsToCommit.remove(0);
				}
			}
		}
	}

	@Override
	public void restoreState(HashMap<KafkaTopicPartition, Long> restoredOffsets) {
		LOG.info("{} (taskIdx={}) restoring offsets from an older version.",
			getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());

		restoredState = restoredOffsets.isEmpty() ? null : restoredOffsets;

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

		if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
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
			} catch (Exception e) {
				if (running) {
					throw e;
				}
				// else ignore exception if we are no longer running
			}
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
			StreamingRuntimeContext runtimeContext,
			OffsetCommitMode offsetCommitMode) throws Exception;

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

	protected abstract boolean getIsAutoCommitEnabled();

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

	/**
	 * Logs the partition information in INFO level.
	 * 
	 * @param logger The logger to log to.
	 * @param partitionInfos List of subscribed partitions
	 */
	protected static void logPartitionInfo(Logger logger, List<KafkaTopicPartition> partitionInfos) {
		Map<String, Integer> countPerTopic = new HashMap<>();
		for (KafkaTopicPartition partition : partitionInfos) {
			Integer count = countPerTopic.get(partition.getTopic());
			if (count == null) {
				count = 1;
			} else {
				count++;
			}
			countPerTopic.put(partition.getTopic(), count);
		}
		StringBuilder sb = new StringBuilder(
				"Consumer is going to read the following topics (with number of partitions): ");
		
		for (Map.Entry<String, Integer> e : countPerTopic.entrySet()) {
			sb.append(e.getKey()).append(" (").append(e.getValue()).append("), ");
		}
		
		logger.info(sb.toString());
	}

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
