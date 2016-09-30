/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.messages.lowwatermark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class of all low watermark service related messages.
 */
public abstract class AbstractLowWatermarkMessage implements java.io.Serializable {

	private static final long serialVersionUID = 6093130113647878298L;

	/** The job to which this low watermark message belongs */
	private final JobID job;

	/** The vertex to which this low watermark message belongs */
	private final JobVertexID vertex;

	/** The task execution that is source/target of the low watermark message */
	private final ExecutionAttemptID taskExecutionID;

	public AbstractLowWatermarkMessage(JobID job,
									JobVertexID vertex,
									ExecutionAttemptID taskExecutionID) {
		this.job = checkNotNull(job);
		this.vertex = checkNotNull(vertex);
		this.taskExecutionID = checkNotNull(taskExecutionID);
	}

	public JobID getJob() {
		return job;
	}

	public JobVertexID getVertex() {
		return vertex;
	}

	public ExecutionAttemptID getTaskExecutionID() {
		return taskExecutionID;
	}

	@Override
	public int hashCode() {
		return job.hashCode()
			+ vertex.hashCode()
			+ taskExecutionID.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof AbstractLowWatermarkMessage) {
			AbstractLowWatermarkMessage that = (AbstractLowWatermarkMessage) obj;
			return this.job.equals(that.job)
				&& this.vertex.equals(that.vertex)
				&& this.taskExecutionID.equals(that.taskExecutionID);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "(" + job + '/' + vertex + '/' + taskExecutionID +')';
	}
}
