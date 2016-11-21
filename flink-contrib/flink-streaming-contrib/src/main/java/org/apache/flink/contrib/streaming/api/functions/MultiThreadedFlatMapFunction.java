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

package org.apache.flink.contrib.streaming.api.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.contrib.streaming.api.operators.ExactlyOnceCollector;
import scala.Serializable;

/**
 * Base interface for multi-threaded flatMap functions. FlatMap functions take elements and transform them,
 * into zero, one, or more elements. Typical applications can be splitting elements, or unnesting lists
 * and arrays.
 *
 * <p>A {@code MultiThreadedFlatMapFunction}, in addition to the functionality of a normal
 * {@link org.apache.flink.api.common.functions.FlatMapFunction}, will be concurrently invoked across multiple threads.
 * This variant may be useful for flatMap applications which require blocking calls to external services (e.x. querying
 * a database), for which if executed sequentially with the normal {@code FlatMapFunction} can result in spending most
 * of the computation time blocking while waiting for external service requests to return.
 *
 * <p>Note that the {@code MultiThreadedFlatMapFunction} has 2 major differences from a normal {@code FlatMapFunction}:
 *
 * <ul>
 *     <li>Outputs can be collected by the provided output collector, {@code ExactlyOnceCollector}, exactly one time.
 *         If there is no output transformed for the invoked input element, you should still provide an empty
 *         collection to be collected.</li>
 *     <li>Each invocation will be passed a checkpoint lock. This lock is only relevant if the function implementation
 *         contains user state to be checkpointed by Flink for exactly-once guarantees. </li>
 * </ul>
 *
 * @param <IN> Type of the input elements
 * @param <OUT> Type of the output elements
 */
public interface MultiThreadedFlatMapFunction<IN, OUT> extends Function, Serializable {

	/**
	 * The core method of the {@code MultiThreadedFlatMapFunction}. Takes an element from the input data set and
	 * transforms it into zero, one, or more elements. The method will be concurrently executed across a fixed-size
	 * thread pool.
	 *
	 * @param value The input value.
	 * @param collector Collector to collect transformed outputs. Note that all outputs should be collected by the
	 *                  collector in a single call to {@code collect(Collection<OUT> outputs)}. If there are no
	 *                  transformed outputs for the invoked input, you should still provide an empty collection.
	 * @param checkpointLock If the implementation contains user state to be checkpointed, updates to the state as will
	 *                       as output collecting must be synchronized with this lock, as an atomic operation. This is
	 *                       required for the function to correctly coordinate with Flink's checkpointing mechanism.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void flatMap(IN value, Object checkpointLock, ExactlyOnceCollector<OUT> collector) throws Exception;

}
