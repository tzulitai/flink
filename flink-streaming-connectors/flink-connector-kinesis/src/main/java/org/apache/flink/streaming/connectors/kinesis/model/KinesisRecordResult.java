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

import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisRecordPublisher;

/**
 * A representation of the result of putting a record to AWS Kinesis. It is basically a wrapper around AWS's
 * {@link PutRecordsResultEntry} included with the result of a AWS Kinesis PutRecords operation. We additionally
 * have an associated id that is identical to this results corresponding {@link KinesisRecordEntry},
 * assigned when the record was added to a {@link KinesisRecordPublisher} instance.
 */
public class KinesisRecordResult {

	private final Long id;
	private final PutRecordsResultEntry resultEntry;

	private final int cachedHash;

	public KinesisRecordResult(Long id, PutRecordsResultEntry resultEntry) {
		this.id = id;
		this.resultEntry = resultEntry;

		this.cachedHash = 37 * (id.hashCode() + resultEntry.hashCode());
	}

	public Long getId() {
		return this.id;
	}

	public PutRecordsResultEntry getResultEntry() {
		return this.resultEntry;
	}

	public String getShardId() {
		return resultEntry.getShardId();
	}

	public String getSequenceNumber() {
		return resultEntry.getSequenceNumber();
	}

	public String getErrorCode() {
		return resultEntry.getErrorCode();
	}

	public String getErrorMessage() {
		return resultEntry.getErrorMessage();
	}

	public boolean isSuccessful() {
		return resultEntry.getSequenceNumber() != null;
	}

	@Override
	public String toString() {
		return "KinesisRecordResult{" +
			"id='" + getId() + "'" +
			", shardId='" + getShardId() + "'" +
			", sequenceNumber='" + getSequenceNumber() + "'" +
			", errorCode='" + getErrorCode() + "'" +
			", errorMessage='" + getErrorMessage() + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KinesisRecordResult)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		KinesisRecordResult other = (KinesisRecordResult) obj;

		return id.equals(other.id) && resultEntry.equals(other.getResultEntry());
	}

	@Override
	public int hashCode() {
		return this.cachedHash;
	}

}
