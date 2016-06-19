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

package org.apache.flink.streaming.connectors.kinesis.manualtests;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.testutils.ExactlyOnceValidatingConsumerThread;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This test first starts a data generator, producing data into kinesis.
 * Then, it starts a consuming topology, ensuring that all records up to a certain
 * point have been seen. While the data generator and consuming topology is running,
 * the kinesis stream is resharded two times.
 *
 * Invocation:
 * --region eu-central-1 --accessKey XXXXXXXXXXXX --secretKey XXXXXXXXXXXXXXXX
 */
public class ManualExactlyOnceWithStreamReshardingTest {

	private static final Logger LOG = LoggerFactory.getLogger(ManualExactlyOnceTest.class);

	static final int TOTAL_EVENT_COUNT = 20000; // a large enough record count so we can test resharding

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);
		LOG.info("Starting exactly once test");

		final String streamName = "flink-test-" + UUID.randomUUID().toString();
		final String accessKey = pt.getRequired("accessKey");
		final String secretKey = pt.getRequired("secretKey");
		final String region = pt.getRequired("region");

		final Properties configProps = new Properties();
		configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, accessKey);
		configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, secretKey);
		configProps.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, region);
		AmazonKinesisClient client = AWSUtil.createKinesisClient(configProps);

		// the stream is first created with 1 shard
		client.createStream(streamName, 1);

		// wait until stream has been created
		DescribeStreamResult status = client.describeStream(streamName);
		LOG.info("status {}", status);
		while(!status.getStreamDescription().getStreamStatus().equals("ACTIVE")) {
			status = client.describeStream(streamName);
			LOG.info("Status of stream {}", status);
			Thread.sleep(1000);
		}

		final Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");

		ForkableFlinkMiniCluster flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		final int flinkPort = flink.getLeaderRPCPort();

		try {
			// we have to use a manual generator here instead of the FlinkKinesisProducer
			// because the FlinkKinesisProducer currently has a problem where records will be resent to a shard
			// when resharding happens; this affects the consumer exactly-once validation test and will never pass
			final AtomicReference<Throwable> producerError = new AtomicReference<>();
			Runnable manualGenerate = new Runnable() {
				@Override
				public void run() {
					AmazonKinesisClient client = AWSUtil.createKinesisClient(configProps);
					int count = 0;
					final int batchSize = 30;
					while (true) {
						try {
							Thread.sleep(10);

							Set<PutRecordsRequestEntry> batch = new HashSet<>();
							for (int i=count; i<count+batchSize; i++) {
								if (i >= TOTAL_EVENT_COUNT) {
									break;
								}
								batch.add(
									new PutRecordsRequestEntry()
										.withData(ByteBuffer.wrap(((i) + "-" + RandomStringUtils.randomAlphabetic(12)).getBytes()))
										.withPartitionKey(UUID.randomUUID().toString()));
							}
							count += batchSize;

							PutRecordsResult result = client.putRecords(new PutRecordsRequest().withStreamName(streamName).withRecords(batch));

							// the putRecords() operation may have failing records; to keep this test simple
							// instead of retrying on failed records, we simply pass on a runtime exception
							// and let this test fail
							if (result.getFailedRecordCount() > 0) {
								producerError.set(new RuntimeException("The producer has failed records in one of the put batch attempts."));
								break;
							}

							if (count >= TOTAL_EVENT_COUNT) {
								break;
							}
						} catch (Exception e) {
							producerError.set(e);
						}
					}
				}
			};
			Thread producerThread = new Thread(manualGenerate);
			producerThread.start();

			final AtomicReference<Throwable> consumerError = new AtomicReference<>();
			Thread consumerThread = ExactlyOnceValidatingConsumerThread.create(
				TOTAL_EVENT_COUNT, 10000, 2, 500, 500,
				accessKey, secretKey, region, streamName,
				consumerError, flinkPort, flinkConfig);
			consumerThread.start();

			// reshard the Kinesis stream while the producer / and consumers are running

			// split the first shard into two, equally partitioning hash range
			Thread.sleep(5000);
			LOG.info("Splitting shard ...");
			client.splitShard(
				streamName,
				KinesisShardIdGenerator.generateFromShardOrder(0),
				"170141183460469231731687303715884105727");

			// then merge again
			Thread.sleep(35000);
			LOG.info("Merging shards ...");
			client.mergeShards(
				streamName,
				KinesisShardIdGenerator.generateFromShardOrder(1),
				KinesisShardIdGenerator.generateFromShardOrder(2));


			boolean deadlinePassed = false;
			long deadline = System.currentTimeMillis() + (1000 * 5 * 60); // wait at most for five minutes
			while (consumerThread.isAlive() && producerThread.isAlive()) {
				Thread.sleep(1000);
				if (System.currentTimeMillis() >= deadline) {
					LOG.warn("Deadline passed");
					deadlinePassed = true;
					break; // enough waiting
				}
			}

			if (producerThread.isAlive()) {
				producerThread.interrupt();
			}

			if (consumerThread.isAlive()) {
				consumerThread.interrupt();
			}

			if (producerError.get() != null) {
				LOG.info("+++ TEST failed! +++");
				throw new RuntimeException("Producer failed", producerError.get());

			}

			if (consumerError.get() != null) {
				LOG.info("+++ TEST failed! +++");
				throw new RuntimeException("Consumer failed", consumerError.get());
			}

			if (!deadlinePassed) {
				LOG.info("+++ TEST passed! +++");
			} else {
				LOG.info("+++ TEST failed! +++");
			}

		} finally {
			client.deleteStream(streamName);
			client.shutdown();

			// stopping flink
			flink.stop();
		}
	}

}
