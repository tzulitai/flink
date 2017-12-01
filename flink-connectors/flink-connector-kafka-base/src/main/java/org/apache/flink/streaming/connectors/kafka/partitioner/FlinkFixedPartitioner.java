/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A partitioner ensuring that each internal Flink partition ends up in one Kafka partition of any given topic.
 * In the case of a target Kafka topic being rescaled and has new partitions, the determined Kafka partition for that
 * topic is guaranteed to remain identical.
 *
 * <p>Note, one Kafka partition can contain multiple Flink partitions.
 *
 * <p>Cases:
 * 	# More Flink partitions than kafka partitions
 * <pre>
 * 		Flink Sinks:		Kafka Partitions
 * 			1	----------------&gt;	1
 * 			2   --------------/
 * 			3   -------------/
 * 			4	------------/
 * </pre>
 * Some (or all) kafka partitions contain the output of more than one flink partition
 *
 * <p>Fewer Flink partitions than Kafka
 * <pre>
 * 		Flink Sinks:		Kafka Partitions
 * 			1	----------------&gt;	1
 * 			2	----------------&gt;	2
 * 										3
 * 										4
 * 										5
 * </pre>
 *
 * <p>Not all Kafka partitions contain data.
 * To avoid such an unbalanced partitioning, use a round-robin kafka partitioner (note that this will
 * cause a lot of network connections between all the Flink instances and all the Kafka brokers).
 */
public class FlinkFixedPartitioner<T> extends FlinkKafkaPartitioner<T> {

	private static final long serialVersionUID = -6002230350113297637L;

	private int parallelInstanceId;
	private int parallelInstances;

	private FixedTargetPartitionsState fixedTargetPartitionsState;

	private ListState<FixedTargetPartitionsState> checkpointedUnionState;

	@Override
	public void open(int parallelInstanceId, int parallelInstances) {
		Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
		Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");

		this.parallelInstanceId = parallelInstanceId;
		this.parallelInstances = parallelInstances;

		this.fixedTargetPartitionsState = new FixedTargetPartitionsState(this.parallelInstanceId);
	}

	@Override
	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		Preconditions.checkArgument(
			partitions != null && partitions.length > 0,
			"Partitions of the target topic is empty.");

		if (fixedTargetPartitionsState.hasFixedTargetPartitionFor(targetTopic)) {
			return fixedTargetPartitionsState.getFixedTargetPartition(targetTopic);
		}

		int targetPartition = partitions[parallelInstanceId % partitions.length];
		fixedTargetPartitionsState.setFixedTargetPartition(targetTopic, targetPartition);

		return targetPartition;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.checkpointedUnionState = context.getOperatorStateStore().getUnionListState(
			new ListStateDescriptor<>("FixedTargetPartitionsState", FixedTargetPartitionsState.class));

		if (context.isRestored()) {
			for (FixedTargetPartitionsState targetPartitionsState : checkpointedUnionState.get()) {
				if (targetPartitionsState.getParallelInstanceId() % parallelInstances == parallelInstanceId) {
					for (Map.Entry<String, Integer> stateEntry : targetPartitionsState.getTopicToFixedPartition().entrySet()) {
						if (!this.fixedTargetPartitionsState.hasFixedTargetPartitionFor(stateEntry.getKey())
								|| targetPartitionsState.getParallelInstanceId() == this.parallelInstanceId) {
							this.fixedTargetPartitionsState.setFixedTargetPartition(stateEntry.getKey(), stateEntry.getValue());
						}
					}
				}
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		this.checkpointedUnionState.clear();
		checkpointedUnionState.add(fixedTargetPartitionsState);
	}

	/**
	 * POJO.
	 */
	public static class FixedTargetPartitionsState {

		private int parallelInstanceId;

		private Map<String, Integer> topicToFixedPartition;

		public FixedTargetPartitionsState() {}

		public FixedTargetPartitionsState(int parallelInstanceId, Map<String, Integer> topicToFixedPartition) {
			this.parallelInstanceId = parallelInstanceId;
			this.topicToFixedPartition = topicToFixedPartition;
		}

		public FixedTargetPartitionsState(int parallelInstanceId) {
			this(parallelInstanceId, new HashMap<>());
		}

		public int getParallelInstanceId() {
			return parallelInstanceId;
		}

		public void setParallelInstanceId(int parallelInstanceId) {
			this.parallelInstanceId = parallelInstanceId;
		}

		public Map<String, Integer> getTopicToFixedPartition() {
			return topicToFixedPartition;
		}

		public void setTopicToFixedPartition(Map<String, Integer> topicToFixedPartition) {
			this.topicToFixedPartition = topicToFixedPartition;
		}

		public boolean hasFixedTargetPartitionFor(String targetTopic) {
			return topicToFixedPartition.containsKey(targetTopic);
		}

		public int getFixedTargetPartition(String targetTopic) {
			return topicToFixedPartition.get(targetTopic);
		}

		public void setFixedTargetPartition(String targetTopic, int targetPartition) {
			this.topicToFixedPartition.put(targetTopic, targetPartition);
		}
	}
}
