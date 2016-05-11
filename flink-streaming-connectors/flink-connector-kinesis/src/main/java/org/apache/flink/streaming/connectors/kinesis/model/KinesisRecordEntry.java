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

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisRecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A representation of a record that is to be put to AWS Kinesis. It is basically a wrapper around
 * AWS's {@link PutRecordsRequestEntry}, additionally with the stream that this record is to be put to, and
 * the id of this record (assigned by the {@link KinesisRecordPublisher} that this record was added to).
 */
public class KinesisRecordEntry {

	private Long id;

	private final String streamName;
	private final PutRecordsRequestEntry entry;

	private final int cachedHash;

	public KinesisRecordEntry(Long id, String streamName, ByteBuffer data, String partition, String explicitHashKey) {
		this.id = Objects.requireNonNull(id);
		this.streamName = Objects.requireNonNull(streamName);

		this.entry = new PutRecordsRequestEntry();
		this.entry.withData(Objects.requireNonNull(data));
		this.entry.withPartitionKey(partition);
		this.entry.withExplicitHashKey(explicitHashKey);

		this.cachedHash = 37 * (id.hashCode() + streamName.hashCode() + entry.hashCode());
	}

	public Long getId() {
		return this.id;
	}

	public String getStreamName() {
		return this.streamName;
	}

	public PutRecordsRequestEntry getEntry() {
		return this.entry;
	}

	public ByteBuffer getData() {
		return this.entry.getData();
	}

	public String getPartitionKey() {
		return this.entry.getPartitionKey();
	}

	public String getExplicitHashKey() {
		return this.entry.getExplicitHashKey();
	}

	@Override
	public String toString() {
		return "KinesisRecordEntry{" +
			"id='" + getId() + "'" +
			", streamName='" + getStreamName() + "'" +
			", data='" + new String(getData().array()) + "'" +
			", partitionKey='" + getPartitionKey() + "'" +
			", explicitHashKey='" + getExplicitHashKey() + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KinesisRecordEntry)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		KinesisRecordEntry other = (KinesisRecordEntry) obj;

		return id.equals(other.getId()) && streamName.equals(other.getStreamName()) && entry.equals(other.getEntry());
	}

	@Override
	public int hashCode() {
		return this.cachedHash;
	}

	/**
	 * Utility method used only by {@link KinesisProxy#putRecords(String, List)}
	 *
	 * @param records list of records to convert
	 * @return converted list of records, all of type {@link PutRecordsRequestEntry}
	 */
	public static List<PutRecordsRequestEntry> convertAllToPutRecordsRequestEntry(List<KinesisRecordEntry> records) {
		List<PutRecordsRequestEntry> converted = new ArrayList<>(records.size());
		for (KinesisRecordEntry record : records) {
			converted.add(record.getEntry());
		}
		return converted;
	}
}
