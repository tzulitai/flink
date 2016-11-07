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
package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.watermark.WatermarkStatus;

/**
 * An Input Channel Status keeps track of an input channel's last watermark, watermark status, and whether or not
 * the channel's current watermark is aligned with the overall watermark advancement.
 *
 * There are 2 situations where a channel's watermark is not considered aligned, and therefore not accounted
 * for when determining if the overall watermark should be advanced:
 *  - the current watermark status of the channel is idle
 *  - the watermark status has resumed to be active, but the current watermark hasn't caught up to the
 *    last emitted watermark yet.
 */
@Internal
public class InputChannelStatus {

	private long watermark;
	private WatermarkStatus watermarkStatus;
	private boolean isWatermarkAligned;

	public InputChannelStatus(long watermark, WatermarkStatus watermarkStatus, boolean isWatermarkAligned) {
		this.watermark = watermark;
		this.watermarkStatus = watermarkStatus;
		this.isWatermarkAligned = isWatermarkAligned;
	}

	public void setWatermark(long watermark) {
		this.watermark = watermark;
	}

	public void setWatermarkStatus(WatermarkStatus watermarkStatus) {
		this.watermarkStatus = watermarkStatus;
	}

	public void setIsWatermarkAligned(boolean isWatermarkAligned) {
		this.isWatermarkAligned = isWatermarkAligned;
	}

	public long getWatermark() {
		return watermark;
	}

	public WatermarkStatus getWatermarkStatus() {
		return watermarkStatus;
	}

	public boolean isWatermarkIdle() {
		return watermarkStatus.isIdle();
	}

	public boolean isWatermarkActive() {
		return watermarkStatus.isActive();
	}

	public boolean isWatermarkAligned() {
		return isWatermarkAligned;
	}

}
