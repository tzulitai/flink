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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
 * Elasticsearch 6.x sink that requests multiple {@link ActionRequest ActionRequests}
 * against a cluster for each incoming element.
 *
 * <p>The sink internally uses a {@link RestHighLevelClient} to communicate with an Elasticsearch cluster.
 * The sink will fail if no cluster can be connected to using the provided transport addresses passed to the constructor.
 *
 * <p>Internally, the sink will use a {@link BulkProcessor} to send {@link ActionRequest ActionRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>You also have to provide an {@link ElasticsearchSinkFunction}. This is used to create multiple
 * {@link ActionRequest ActionRequests} for each incoming element. See the class level documentation of
 * {@link ElasticsearchSinkFunction} for an example.
 *
 * @param <T> Type of the elements handled by this sink
 */
@PublicEvolving
public class ElasticsearchSink<T> extends ElasticsearchSinkBase<T, RestHighLevelClient> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
	 *
	 * <p>This constructor uses the default configurations of the {@link RestHighLevelClient}. For custom
	 * configuration, please use the {@link #ElasticsearchSink(Map, List, ElasticsearchSinkFunction, RestClientFactory)}
	 * constructor.
	 *
	 * @param userConfig user configuration to configure bulk flushing behaviour.
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element
	 * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
	 */
	public ElasticsearchSink(Map<String, String> userConfig, List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {

		this(
			userConfig,
			httpHosts,
			elasticsearchSinkFunction,
			restClientBuilder -> {} // a no-op rest client factory, that uses the default configurations
		);
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
	 *
	 * @param userConfig user configuration to configure bulk flushing behaviour.
	 * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element.
	 * @param restClientFactory the factory that configures the rest client.
	 */
	public ElasticsearchSink(
		Map<String, String> userConfig,
		List<HttpHost> httpHosts,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		RestClientFactory restClientFactory) {

		this(
			userConfig,
			httpHosts,
			elasticsearchSinkFunction,
			new NoOpFailureHandler(),
			restClientFactory);
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
	 *
	 * @param userConfig user configuration to configure bulk flushing behaviour.
	 * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element.
	 * @param failureHandler This is used to handle failed {@link ActionRequest}.
	 * @param restClientFactory the factory that configures the rest client.
	 */
	public ElasticsearchSink(
		Map<String, String> userConfig,
		List<HttpHost> httpHosts,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		ActionRequestFailureHandler failureHandler,
		RestClientFactory restClientFactory) {

		super(new Elasticsearch6ApiCallBridge(httpHosts, restClientFactory),  userConfig, elasticsearchSinkFunction, failureHandler);
	}
}
