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

package org.apache.flink.streaming.connectors.kafka.config;

public class OffsetCommitModes {

	/**
	 *
	 * @param enableAutoCommit
	 * @param enableCommitOnCheckpoint
	 * @param enableCheckpointing
	 * @return
	 */
	public static OffsetCommitMode fromConfiguration(
			boolean enableAutoCommit,
			boolean enableCommitOnCheckpoint,
			boolean enableCheckpointing) {

		// committing on checkpoints is only possible when checkpointing
		// is enabled and commitOnCheckpoint is set to true
		if (enableCheckpointing) {
			return (enableCommitOnCheckpoint) ? OffsetCommitMode.ON_CHECKPOINTS : OffsetCommitMode.DISABLED;
		} else {
			return (enableAutoCommit) ? OffsetCommitMode.KAFKA_PERIODIC : OffsetCommitMode.DISABLED;
		}
	}

}
