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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all partition discoverers.
 *
 * <p>This partition discoverer base class implements the logic around bookkeeping
 * discovered partitions, and using the information to determine whether or not there
 * are new partitions that the consumer subtask should subscribe to.
 *
 * <p>Kafka version-specific implementations need to implement the actual connections
 * to Kafka brokers and how to fetch topic and partition metadata from Kafka.
 *
 * <p>Partition discovers may be
 */
public abstract class AbstractPartitionDiscoverer {

	/** Describes whether we are discovering partitions for fixed topics or a topic pattern. */
	private final KafkaTopicsDescriptor topicsDescriptor;

	/** Index of the consumer subtask that this partition discoverer belongs to. */
	private final int indexOfThisSubtask;

	/** The total number of consumer subtasks. */
	private final int numParallelSubtasks;

	/**
	 * Map of topics to they're largest discovered partition id seen by this subtask.
	 * This state may be updated whenever {@link AbstractPartitionDiscoverer#discoverPartitions()} or
	 * {@link AbstractPartitionDiscoverer#checkAndSetDiscoveredPartition(KafkaTopicPartition)} is called.
	 *
	 * This is used to remove old partitions from the fetched partition lists. It is sufficient
	 * to keep track of only the largest partition id because Kafka partition numbers are only
	 * allowed to be increased and has incremental ids.
	 */
	private final Map<String, Integer> topicsToLargestDiscoveredPartitionId;

	public AbstractPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks) {

		this.topicsDescriptor = checkNotNull(topicsDescriptor);
		this.indexOfThisSubtask = indexOfThisSubtask;
		this.numParallelSubtasks = numParallelSubtasks;
		this.topicsToLargestDiscoveredPartitionId = new HashMap<>();
	}

	/**
	 * Execute a partition discovery attempt for this subtask.
	 * This method lets the partition discoverer update what partitions it has discovered so far.
	 *
	 * @return List of discovered new partitions that this subtask should subscribe to.
	 */
	public List<KafkaTopicPartition> discoverPartitions() throws Exception {
		List<KafkaTopicPartition> newDiscoveredPartitions;

		// (1) get all possible partitions, based on whether we are subscribed to fixed topics or a topic patern
		if (topicsDescriptor.isFixedTopics()) {
			newDiscoveredPartitions = getAllPartitionsForTopics(topicsDescriptor.getFixedTopics());
		} else {
			List<String> matchedTopics = getAllTopics();

			// retain topics that match the pattern
			Iterator<String> iter = matchedTopics.iterator();
			while (iter.hasNext()) {
				if (!topicsDescriptor.getTopicPattern().matcher(iter.next()).matches()) {
					iter.remove();
				}
			}

			if (matchedTopics.size() != 0) {
				// get partitions only for matched topics
				newDiscoveredPartitions = getAllPartitionsForTopics(matchedTopics);
			} else {
				newDiscoveredPartitions = null;
			}
		}

		// (2) eliminate partition that are old partitions or should not be subscribed by this subtask
		if (newDiscoveredPartitions == null || newDiscoveredPartitions.isEmpty()) {
			throw new RuntimeException("Unable to retrieve any partitions with KafkaTopicsDescriptor: " + topicsDescriptor);
		} else {
			// sort so that we make sure the topicsToLargestDiscoveredPartitionId state is updated
			// with incremental partition ids of the same topics (otherwise some partition ids may be skipped)
			Collections.sort(newDiscoveredPartitions, new Comparator<KafkaTopicPartition>() {
				@Override
				public int compare(KafkaTopicPartition o1, KafkaTopicPartition o2) {
					if (!o1.getTopic().equals(o2.getTopic())) {
						return o1.getTopic().compareTo(o2.getTopic());
					} else {
						return Integer.compare(o1.getPartition(), o2.getPartition());
					}
				}
			});

			Iterator<KafkaTopicPartition> iter = newDiscoveredPartitions.iterator();
			KafkaTopicPartition nextPartition;
			while (iter.hasNext()) {
				nextPartition = iter.next();
				if (!checkAndSetDiscoveredPartition(nextPartition) ||
						!shouldAssignToThisSubtask(nextPartition, indexOfThisSubtask, numParallelSubtasks)) {
					iter.remove();
				}
			}
		}

		return newDiscoveredPartitions;
	}

	/**
	 * Checks whether the given partition has been discovered yet.
	 * If it hasn't been discovered yet, this method lets the partition discoverer update what
	 * partitions it has discovered so far.
	 *
	 * @param partition the partition to check and set
	 *
	 * @return {@code true}, if the partition wasn't seen before, {@code false} otherwise
	 */
	public boolean checkAndSetDiscoveredPartition(KafkaTopicPartition partition) {
		if (!topicsToLargestDiscoveredPartitionId.containsKey(partition.getTopic())
				|| partition.getPartition() > topicsToLargestDiscoveredPartitionId.get(partition.getTopic())) {

			topicsToLargestDiscoveredPartitionId.put(partition.getTopic(), partition.getPartition());
			return true;
		}

		return false;
	}

	// ------------------------------------------------------------------------
	//  Kafka version specifics
	// ------------------------------------------------------------------------

	/** Establish the required connections in order to fetch topics and partitions metadata. */
	public abstract void initializeConnections() throws Exception;

	/** Close all established connections. */
	public abstract void closeConnections() throws Exception;

	/** Fetch the list of all topics from Kafka. */
	protected abstract List<String> getAllTopics() throws Exception;

	/** Fetch the list of all partitions for a specific topics list from Kafka. */
	protected abstract List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) throws Exception;

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static boolean shouldAssignToThisSubtask(KafkaTopicPartition partition, int indexOfThisSubtask, int numParallelSubtasks) {
		return Math.abs(partition.hashCode() % numParallelSubtasks) == indexOfThisSubtask;
	}

	@VisibleForTesting
	Map<String, Integer> getTopicsToLargestDiscoveredPartitionId() {
		return topicsToLargestDiscoveredPartitionId;
	}
}
