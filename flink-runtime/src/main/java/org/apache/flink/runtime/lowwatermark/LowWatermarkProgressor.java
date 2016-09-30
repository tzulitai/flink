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
package org.apache.flink.runtime.lowwatermark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.lowwatermark.NotifyLowWatermarkProgress;
import org.apache.flink.runtime.messages.lowwatermark.ReportLowWatermark;
import org.apache.flink.runtime.messages.lowwatermark.RetrieveLowWatermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The low watermark progressor keeps track of
 */
public class LowWatermarkProgressor {

	protected static final Logger LOG = LoggerFactory.getLogger(LowWatermarkProgressor.class);

	private final Object lock = new Object();

	/** */
	private final JobID job;

	/** */
	private final JobVertexID vertex;

	/** */
	private final ExecutionVertex[] participatingTasks;

	/** */
	private long lastAggregatedLowWatermark;

	/** */
	private final Timer timer;

	/** Actor that receives status updates from the execution graph this progressor works for */
	private JobStatusListener jobStatusListener;

	private final long triggerInterval;

	/** */
	private PendingLowWatermark pendingLowWatermark;

	private volatile boolean inProgress;

	private volatile boolean shutdown;

	public LowWatermarkProgressor(
		JobID job,
		JobVertexID vertex,
		ExecutionVertex[] tasks,
		long triggerInterval) {

		this.job = checkNotNull(job);
		this.vertex = checkNotNull(vertex);
		this.participatingTasks = checkNotNull(tasks);

		checkArgument(triggerInterval > 0, "");

		this.timer = new Timer("Low Watermark Progressor Timer", true);

		this.triggerInterval = triggerInterval;
	}

	private void triggerLowWatermarkRetrieval() {
		if (shutdown) {
			return;
		}

		if (!inProgress) {
			// check if all tasks are running
			ExecutionAttemptID[] triggerTaskIDs = new ExecutionAttemptID[participatingTasks.length];

			synchronized (lock) {
				for (int i = 0; i < participatingTasks.length; i++) {
					Execution execution = participatingTasks[i].getCurrentExecutionAttempt();
					if (execution != null && execution.getState() == ExecutionState.RUNNING) {
						triggerTaskIDs[i] = execution.getAttemptId();
					} else {
						// some tasks are not running ...
					}
				}

				this.pendingLowWatermark = new PendingLowWatermark(
					new HashSet<>(Arrays.asList(triggerTaskIDs)),
					this.lastAggregatedLowWatermark);
			}

			for (int i = 0; i < participatingTasks.length; i++) {
				ExecutionAttemptID id = triggerTaskIDs[i];
				RetrieveLowWatermark message = new RetrieveLowWatermark(job, vertex, id);
				participatingTasks[i].sendMessageToCurrentExecution(message, id);
			}

			inProgress = true;
		} else {
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!");
			LOG.warn("skipping ...");
		}

	}

	// -------------- Message handling -------------------

	public boolean receiveLowWatermarkReportMessage(ReportLowWatermark lowWatermarkReport) {

		if (!lowWatermarkReport.getJob().equals(this.job)) {
			return false;
		}

		if (!lowWatermarkReport.getVertex().equals(this.vertex)) {
			return false;
		}

		ExecutionAttemptID task = lowWatermarkReport.getTaskExecutionID();
		Long aggregatedLowWatermark = null;

		synchronized (lock) {
			if (shutdown) {
				return false;
			}

			pendingLowWatermark.aggregate(task, lowWatermarkReport.getReportedLowWatermark());

			if (pendingLowWatermark.isComplete()) {
				aggregatedLowWatermark = pendingLowWatermark.getAggregatedLowWatermark();
			}
		}

		if (aggregatedLowWatermark != null) {
			if (aggregatedLowWatermark > this.lastAggregatedLowWatermark) {
				this.lastAggregatedLowWatermark = aggregatedLowWatermark;
				for (ExecutionVertex v : participatingTasks) {
					Execution execution = v.getCurrentExecutionAttempt();
					if (execution != null) {
						ExecutionAttemptID attemptId = execution.getAttemptId();
						NotifyLowWatermarkProgress notifyMessage = new NotifyLowWatermarkProgress(job, this.vertex, attemptId, this.lastAggregatedLowWatermark);
						v.sendMessageToCurrentExecution(notifyMessage, attemptId);
					}
				}
			}

			inProgress = false;
		}

		return true;
	}

	// ---------------------------------------------------

	public void startProgressor() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			// make sure all prior timers are cancelled
			stopProgressor();

			shutdown = false;
			inProgress = false;
			timer.scheduleAtFixedRate(new LowWatermarkRetrievalTrigger(), triggerInterval, triggerInterval);
		}
	}

	public void stopProgressor() {
		synchronized (lock) {

			if (inProgress) {
				LOG.warn("suspending ...");
			}

			pendingLowWatermark = null;
			shutdown = true;
		}
	}

	// ---------------------------------------------------

	public JobStatusListener createActivatorDeactivator() {
		synchronized (lock) {
			if (shutdown) {
				throw new RuntimeException("Low Watermark Progressor is already shut down.");
			}

			if (jobStatusListener == null) {
				jobStatusListener = new LowWatermarkProgressorDeActivator(this);
			}

			return jobStatusListener;
		}
	}

	private class LowWatermarkRetrievalTrigger extends TimerTask {
		@Override
		public void run() {
			try {
				triggerLowWatermarkRetrieval();
			} catch (Exception e) {
				LOG.error("Exception while triggering low watermark retrieval", e);
			}
		}
	}

}
