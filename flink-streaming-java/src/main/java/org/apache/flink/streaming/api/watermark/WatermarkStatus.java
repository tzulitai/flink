/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * A Watermark Status element informs receiving operators whether or not they should continue to expect watermarks from
 * the sending operator. There are 2 kinds of status, namely {@link WatermarkStatus#IDLE} and
 * {@link WatermarkStatus#ACTIVE}. Watermark Status elements are generated at the sources, and may be propagated through
 * the operators of the topology using {@link org.apache.flink.streaming.api.operators.Output#emitWatermarkStatus(WatermarkStatus)}.
 * Sources and downstream operators should emit either of the status elements once it changes between "watermark-idle"
 * and "watermark-active" states.
 *
 * <p>
 * A source is considered "watermark-idle" if it will not emit records for an indefinite amount of time. This is the
 * case, for example, for Flink's Kafka Consumer, where sources might initially have no assigned partitions to
 * read from, or no records can be read from the assigned partitions. Once the source detects that it will resume emitting
 * data, it is considered "watermark-active".
 *
 * <p>
 * Downstream operators with multiple inputs (ex. head operators of a
 * {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask} or
 * {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}) should not wait for watermarks from an upstream
 * operator that is "watermark-idle" when deciding whether or not to advance the operator's current watermark.
 * When a downstream operator determines that all upstream operators are "watermark-idle" (i.e. when all input channels
 * have received the watermark idle status element), then the operator is considered to also be "watermark-idle",
 * as it will temporarily be unable to advance its own watermark. This is always the case for operators that only read
 * from a single upstream operator. Once an operator is considered "watermark-idle", it should itself forward its idle
 * status to inform downstream operators. The operator is considered to be back to "watermark-active" as soon as at
 * least one of its upstream operators resume to be "watermark-active" (i.e. when at least one input channel receives the
 * watermark active status element), and should also forward its active status to inform downstream operators.
 */
@PublicEvolving
public final class WatermarkStatus extends StreamElement {

	private static final int IDLE_STATUS = 0;
	private static final int ACTIVE_STATUS = -1;

	public static final WatermarkStatus IDLE = new WatermarkStatus(IDLE_STATUS);
	public static final WatermarkStatus ACTIVE = new WatermarkStatus(ACTIVE_STATUS);

	public final int status;

	public WatermarkStatus(int status) {
		if (status != IDLE_STATUS && status != ACTIVE_STATUS) {
			throw new IllegalArgumentException();
		}

		this.status = status;
	}

	public boolean isIdle() {
		return this.status == IDLE_STATUS;
	}

	public boolean isActive() {
		return !isIdle();
	}

	@Override
	public boolean equals(Object o) {
		return this == o ||
			o != null && o.getClass() == WatermarkStatus.class && ((WatermarkStatus) o).status == this.status;
	}

	@Override
	public int hashCode() {
		return status;
	}

	@Override
	public String toString() {
		String statusStr = (status == ACTIVE_STATUS) ? "ACTIVE" : "IDLE";
		return "WatermarkStatus(" + statusStr + ")";
	}
}
