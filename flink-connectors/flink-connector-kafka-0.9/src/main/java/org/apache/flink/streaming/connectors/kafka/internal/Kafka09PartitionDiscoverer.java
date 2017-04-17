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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka 0.9 high-level consumer API.
 *
 * Thread safe?
 */
public class Kafka09PartitionDiscoverer extends AbstractPartitionDiscoverer {

	private final Properties kafkaProperties;

	private KafkaConsumer<?, ?> kafkaConsumer;

	private final Object consumerLock = new Object();

	public Kafka09PartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks,
			Properties kafkaProperties) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);
		this.kafkaProperties = checkNotNull(kafkaProperties);
	}

	@Override
	public void initializeConnections() {
		this.kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
	}

	@Override
	protected List<String> getAllTopics() {
		synchronized (consumerLock) {
			return new ArrayList<>(kafkaConsumer.listTopics().keySet());
		}
	}

	@Override
	protected List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) {
		synchronized (consumerLock) {
			List<KafkaTopicPartition> partitions = new LinkedList<>();

			for (String topic : topics) {
				for (PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
					partitions.add(new KafkaTopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}
			}

			return partitions;
		}
	}

	@Override
	public void closeConnections() throws Exception {
		synchronized (consumerLock) {
			if (this.kafkaConsumer != null) {
				KafkaConsumer<?, ?> consumer = this.kafkaConsumer;
				consumer.close();

				// make sure we don't re-close an already closed consumer
				this.kafkaConsumer = null;
			}
		}
	}
}
