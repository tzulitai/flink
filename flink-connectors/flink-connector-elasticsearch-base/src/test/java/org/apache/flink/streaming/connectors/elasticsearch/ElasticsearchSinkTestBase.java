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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.InstantiationUtil;

import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Environment preparation and suite of tests for version-specific {@link ElasticsearchSinkBase} implementations.
 *
 * @param <C> Elasticsearch client type
 * @param <A> The address type to use
 */
public abstract class ElasticsearchSinkTestBase<C extends AutoCloseable, A> extends AbstractTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkTestBase.class);

	protected static final String CLUSTER_NAME = "test-cluster";

	protected static EmbeddedElasticsearchNodeEnvironment embeddedNodeEnv;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void prepare() throws Exception {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		// dynamically load version-specific implementation of the Elasticsearch embedded node environment
		Class<?> clazz = Class.forName(
			"org.apache.flink.streaming.connectors.elasticsearch.EmbeddedElasticsearchNodeEnvironmentImpl");
		embeddedNodeEnv = (EmbeddedElasticsearchNodeEnvironment) InstantiationUtil.instantiate(clazz);

		embeddedNodeEnv.start(tempFolder.newFolder(), CLUSTER_NAME);

	}

	@AfterClass
	public static void shutdown() throws Exception {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shutting down embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		embeddedNodeEnv.close();

	}

	/**
	 * Tests that the Elasticsearch sink works properly.
	 */
	public void runElasticsearchSinkTest() throws Exception {
		final String index = "transport-client-test-index";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", CLUSTER_NAME);

		source.addSink(createElasticsearchSinkForEmbeddedNode(
			userConfig, new SourceSinkDataTestKit.TestElasticsearchSinkFunction(index)));

		env.execute("Elasticsearch TransportClient Test");

		// verify the results
		Client client = embeddedNodeEnv.getClient();
		SourceSinkDataTestKit.verifyProducedSinkData(client, index);

		client.close();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of addresses is {@code null}.
	 */
	public void runNullAddressesTest() throws Exception {
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", "my-transport-client-cluster");

		try {
			createElasticsearchSink(userConfig, null, new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test"));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of addresses is empty.
	 */
	public void runEmptyAddressesTest() throws Exception {
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", "my-transport-client-cluster");

		try {
			createElasticsearchSink(
				userConfig,
				Collections.emptyList(),
				new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test"));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests whether the Elasticsearch sink fails when there is no cluster to connect to.
	 */
	public void runInvalidElasticsearchClusterTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", "my-transport-client-cluster");

		source.addSink(createElasticsearchSinkForEmbeddedNode(
			Collections.unmodifiableMap(userConfig), new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test")));

		try {
			env.execute("Elasticsearch Transport Client Test");
		} catch (JobExecutionException expectedException) {
			expectedException.printStackTrace();
			return;
		}

		fail();
	}

	/** Creates a version-specific Elasticsearch sink, using arbitrary transport addresses. */
	protected abstract ElasticsearchSinkBase<Tuple2<Integer, String>, C> createElasticsearchSink(
			Map<String, String> userConfig,
			List<A> addresses,
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction);

	/**
	 * Creates a version-specific Elasticsearch sink to connect to a local embedded Elasticsearch node.
	 *
	 * <p>This case is singled out from {@link ElasticsearchSinkTestBase#createElasticsearchSink(Map, List, ElasticsearchSinkFunction)}
	 * because the Elasticsearch Java API to do so is incompatible across different versions.
	 */
	protected abstract ElasticsearchSinkBase<Tuple2<Integer, String>, C> createElasticsearchSinkForEmbeddedNode(
		Map<String, String> userConfig, ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) throws Exception;
}
