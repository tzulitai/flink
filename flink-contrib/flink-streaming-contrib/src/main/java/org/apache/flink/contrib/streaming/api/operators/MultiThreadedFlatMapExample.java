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

package org.apache.flink.contrib.streaming.api.operators;

import org.apache.flink.contrib.streaming.api.functions.MultiThreadedFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;

public class MultiThreadedFlatMapExample {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source = see.addSource(new SourceFunction<String>() {
			int i = 0;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (true) {
					Thread.sleep(10);
					i++;
					ctx.collect(String.valueOf(i));
				}
			}

			@Override
			public void cancel() {

			}
		}).setParallelism(1);

		source.transform("mtfm", source.getType(), new StreamMultiThreadedFlatMap<>(
			new MultiThreadedFlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Object checkpointLock, ExactlyOnceCollector<String> collector) throws Exception {
				Thread.sleep(150);
				collector.collect(Collections.singletonList(String.valueOf(Integer.valueOf(value)*2)));
			}
		}, 3)).print();

		see.execute();
	}

}
