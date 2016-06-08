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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.testutils.ReferenceKinesisShardTopologies;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableFlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Suite of FlinkKinesisConsumer tests, including utility static method tests,
 * and tests for the methods called throughout the source life cycle with mocked KinesisProxy.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkKinesisConsumer.class, KinesisConfigUtil.class})
public class FlinkKinesisConsumerTest {

	@Rule
	private ExpectedException exception = ExpectedException.none();

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.validatePropertiesConfig() tests
	// ----------------------------------------------------------------------

	@Test
	public void testMissingAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The AWS region ('" + KinesisConfigConstants.CONFIG_AWS_REGION + "') must be set in the config.");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnrecognizableAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS region");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "wrongRegionId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testCredentialProviderTypeDefaultToBasicButNoCredentialsSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Please set values for AWS Access Key ID ('"+KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID+"') " +
				"and Secret Key ('" + KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY + "') when using the BASIC AWS credential provider type.");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Please set values for AWS Access Key ID ('"+KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID+"') " +
				"and Secret Key ('" + KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY + "') when using the BASIC AWS credential provider type.");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnrecognizableCredentialProviderTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS Credential Provider Type");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "wrongProviderType");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnrecognizableStreamInitPositionTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid initial position in stream");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "wrongInitPosition");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForDescribeStreamRetryCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for describeStream stream operation retry count");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_RETRIES, "unparsableInt");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForDescribeStreamBackoffMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for describeStream stream operation backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_DESCRIBE_BACKOFF, "unparsableLong");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetRecordsMaxCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum records per getRecords shard operation");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_SHARD_RECORDS_PER_GET, "unparsableInt");

		KinesisConfigUtil.validateConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// Tests for open() source life cycle method
	// ----------------------------------------------------------------------

	@Test
	public void testOpenWithNoRestoreStateFetcherAdvanceToLatestSentinelSequenceNumberWhenConfigSetToStartFromLatest() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "LATEST");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		dummyConsumer.open(new Configuration());

		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(shard, SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.toString());
		}

	}

	@Test
	public void testOpenWithNoRestoreStateFetcherAdvanceToEarliestSentinelSequenceNumberWhenConfigSetToTrimHorizon() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "TRIM_HORIZON");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		dummyConsumer.open(new Configuration());

		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(shard, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.toString());
		}

	}

	@Test
	public void testOpenWithRestoreStateFetcherAdvanceToCorrespondingSequenceNumbers() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "TRIM_HORIZON");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		// generate random UUIDs as sequence numbers of last checkpointed state for each assigned shard
		ArrayList<String> listOfSeqNumIfAssignedShards = new ArrayList<>(fakeAssignedShardsToThisConsumerTask.size());
		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			listOfSeqNumIfAssignedShards.add(UUID.randomUUID().toString());
		}

		HashMap<KinesisStreamShard, String> fakeRestoredState = new HashMap<>();
		for (int i=0; i<fakeAssignedShardsToThisConsumerTask.size(); i++) {
			fakeRestoredState.put(fakeAssignedShardsToThisConsumerTask.get(i), listOfSeqNumIfAssignedShards.get(i));
		}

		dummyConsumer.restoreState(fakeRestoredState);
		dummyConsumer.open(new Configuration());

		for (int i=0; i<fakeAssignedShardsToThisConsumerTask.size(); i++) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(
				fakeAssignedShardsToThisConsumerTask.get(i),
				listOfSeqNumIfAssignedShards.get(i));
		}
	}

	private TestableFlinkKinesisConsumer getDummyConsumerWithMockedKinesisProxy(
		int fakeNumFlinkConsumerTasks,
		int fakeThisConsumerTaskIndex,
		String fakeThisConsumerTaskName,
		List<KinesisStreamShard> fakeCompleteShardList,
		List<KinesisStreamShard> fakeAssignedShardListToThisConsumerTask,
		Properties consumerTestConfig,
		KinesisDataFetcher fetcher,
		HashMap<KinesisStreamShard, String> lastSequenceNumsToRestore,
		boolean hasDiscoveredShards,
		boolean running) {

		final String dummyKinesisStreamName = "flink-test";

		final List<String> dummyKinesisStreamList = Collections.singletonList(dummyKinesisStreamName);

		final KinesisProxy kinesisProxyMock = mock(KinesisProxy.class);

		// mock KinesisProxy that is instantiated in the constructor, as well as its getShardList call
		try {
			whenNew(KinesisProxy.class).withArguments(consumerTestConfig).thenReturn(kinesisProxyMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisProxy in tests", e);
		}

		when(kinesisProxyMock.getShardList(dummyKinesisStreamList)).thenReturn(fakeCompleteShardList);

		List<String> fakeStreams = Collections.singletonList(dummyKinesisStreamName);

		TestableFlinkKinesisConsumer dummyConsumer =
			new TestableFlinkKinesisConsumer(fakeStreams, fakeNumFlinkConsumerTasks,
				fakeThisConsumerTaskIndex, fakeThisConsumerTaskName, consumerTestConfig);

		try {
			Field fetcherField = FlinkKinesisConsumer.class.getDeclaredField("fetcher");
			fetcherField.setAccessible(true);
			fetcherField.set(dummyConsumer, fetcher);

			Field lastSequenceNumsField = FlinkKinesisConsumer.class.getDeclaredField("lastSequenceNums");
			lastSequenceNumsField.setAccessible(true);
			lastSequenceNumsField.set(dummyConsumer, lastSequenceNumsToRestore);

			Field hasDiscoveredShardsField = FlinkKinesisConsumer.class.getDeclaredField("hasDiscoveredShards");
			hasDiscoveredShardsField.setAccessible(true);
			hasDiscoveredShardsField.set(dummyConsumer, hasDiscoveredShards);

			Field runningField = FlinkKinesisConsumer.class.getDeclaredField("running");
			runningField.setAccessible(true);
			runningField.set(dummyConsumer, running);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			// no reason to end up here ...
			throw new RuntimeException(e);
		}

		// mock FlinkKinesisConsumer utility static methods
		mockStatic(FlinkKinesisConsumer.class);
		mockStatic(KinesisConfigUtil.class);

		try {
			// assume static method is correct by mocking
			PowerMockito.when(
				FlinkKinesisConsumer.discoverShardsToConsume(
					fakeStreams,
					fakeNumFlinkConsumerTasks,
					fakeThisConsumerTaskIndex,
					consumerTestConfig))
				.thenReturn(fakeAssignedShardListToThisConsumerTask);

			// assume validatePropertiesConfig static method is correct by mocking
			PowerMockito.doNothing().when(KinesisConfigUtil.class, "validateConfiguration", Mockito.any(Properties.class));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error when power mocking static methods of FlinkKinesisConsumer", e);
		}

		return dummyConsumer;
	}
}
