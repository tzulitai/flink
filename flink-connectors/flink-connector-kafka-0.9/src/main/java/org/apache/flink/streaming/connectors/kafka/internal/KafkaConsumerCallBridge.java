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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * The ConsumerCallBridge simply calls methods on the {@link KafkaConsumer}.
 * 
 * This indirection is necessary, because Kafka broke binary compatibility between 0.9 and 0.10,
 * for example changing {@code assign(List)} to {@code assign(Collection)}.
 * 
 * Because of that, we need to two versions whose compiled code goes against different method signatures.
 * Even though the source of subclasses may look identical, the byte code will be different, because they
 * are compiled against different dependencies.
 */
public class KafkaConsumerCallBridge {

	public void assignPartitions(KafkaConsumer<?, ?> consumer, List<TopicPartition> topicPartitions) throws Exception {
		consumer.assign(topicPartitions);
	}

	public void seekPartitionsToBeginning(KafkaConsumer<?, ?> consumer, List<TopicPartition> partitions) {
		consumer.seekToBeginning(partitions.toArray(new TopicPartition[partitions.size()]));
	}

	public void seekPartitionsToEnd(KafkaConsumer consumer, List<TopicPartition> partitions) {
		consumer.seekToEnd(partitions.toArray(new TopicPartition[partitions.size()]));
	}

}
