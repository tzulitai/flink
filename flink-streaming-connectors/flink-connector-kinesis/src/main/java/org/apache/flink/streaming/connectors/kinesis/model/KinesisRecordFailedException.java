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
 * An exception wrapped around a failed {@link KinesisRecordResult}
 */
public class KinesisRecordFailedException extends Exception {

	private static final long serialVersionUID = -3625166627399407170L;

	private KinesisRecordResult result;

	public KinesisRecordFailedException(KinesisRecordResult result) {

		// we shouldn't be wrapping successful results as failed exceptions
		if (result.isSuccessful()) {
			throw new IllegalArgumentException("Incorrectly wrapping successful record results as exceptions");
		}

		this.result = result;
	}

	@Override
	public String getMessage() {
		return "Kinesis record failed with error code " + result.getErrorCode() + ", reason: " + result.getErrorMessage();
	}
}
