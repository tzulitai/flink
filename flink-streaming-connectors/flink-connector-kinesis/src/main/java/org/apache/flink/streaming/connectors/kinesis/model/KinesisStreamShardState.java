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

package org.apache.flink.streaming.connectors.kinesis.model;

/**
 * A wrapper class that bundles a {@link KinesisStreamShard} with its last processed sequence number.
 */
public class KinesisStreamShardState {

	private KinesisStreamShard shard;
	private String lastProcessedSequenceNum;

	public KinesisStreamShardState(KinesisStreamShard shard, String lastProcessedSequenceNum) {
		this.shard = shard;
		this.lastProcessedSequenceNum = lastProcessedSequenceNum;
	}

	public KinesisStreamShard getShard() {
		return this.shard;
	}

	public String getLastProcessedSequenceNum() {
		return this.lastProcessedSequenceNum;
	}

	public void setLastProcessedSequenceNum(String update) {
		this.lastProcessedSequenceNum = update;
	}

	@Override
	public String toString() {
		return "KinesisStreamShardState{" +
			"shard='" + shard.toString() + "'" +
			", lastProcessedSequenceNumber='" + lastProcessedSequenceNum + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KinesisStreamShardState)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		KinesisStreamShardState other = (KinesisStreamShardState) obj;

		return shard.equals(other.getShard()) && lastProcessedSequenceNum.equals(other.getLastProcessedSequenceNum());
	}

	@Override
	public int hashCode() {
		return 37 * (shard.hashCode() + lastProcessedSequenceNum.hashCode());
	}
}
