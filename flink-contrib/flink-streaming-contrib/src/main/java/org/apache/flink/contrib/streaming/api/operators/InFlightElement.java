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

package org.apache.flink.contrib.streaming.api.operators;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An In-Flight Element is an element buffered by a {@link StreamMultiThreadedFlatMap} operator. It can be either a
 * {@link StreamRecord}, {@link Watermark}, or {@link LatencyMarker}.
 *
 * <p>A {@link StreamRecord} is buffered until its output is collected from the thread processing it. They may also be
 * buffered if the operator received it after an earlier Watermark which cannot yet be emitted.
 *
 * <p>A {@link Watermark} or {@link LatencyMarker} is buffered until all buffered Stream Records that the operator
 * received before it has been emitted.
 *
 * @param <OUT> Type of the collected outputs (only relevant if the element is a {@link StreamRecord}).
 */
public class InFlightElement<OUT> {

	private final StreamElement element;
	private Collection<OUT> outputs;

	public InFlightElement(StreamElement element) {
		this.element = checkNotNull(element);
	}

	public void setOutputCollection(Collection<OUT> outputs) {
		checkNotNull(outputs);

		if (this.element.isRecord()) {
			if (this.outputs == null) {
				this.outputs = outputs;
			} else {
				throw new IllegalStateException("Output already set for in-flight element.");
			}
		} else {
			throw new IllegalStateException("Setting outputs is only allowed for Stream Record in-flight elements.");
		}
	}

	public StreamElement getElement() {
		return element;
	}

	public Collection<OUT> getOutputs() {
		return outputs;
	}

	public boolean isOutputCollected() {
		if (element.isRecord()) {
			return (outputs != null);
		} else {
			return true;
		}
	}

	public static <IN, OUT> void serialize(
			InFlightElement<OUT> element,
			DataOutputView outputView,
			StreamElementSerializer<IN> inSerializer,
			StreamElementSerializer<OUT> outSerializer) throws IOException {

		inSerializer.serialize(element.getElement(), outputView);

		Collection<OUT> outputs = element.getOutputs();
		if (outputs == null) {
			outputView.writeInt(-1);
		} else {
			outputView.writeInt(outputs.size());
			for (OUT outputValue : outputs) {
				outSerializer.serialize(new StreamRecord<>(outputValue), outputView);
			}
		}
	}

	public static <IN, OUT> InFlightElement<OUT> deserialize(
			DataInputView inputView,
			StreamElementSerializer<IN> inSerializer,
			StreamElementSerializer<OUT> outSerializer) throws IOException {

		StreamElement element = inSerializer.deserialize(inputView);
		int numOutputs = inputView.readInt();

		InFlightElement<OUT> deserialized = new InFlightElement<>(element);
		if (numOutputs >= 0) {
			List<OUT> outputs = new ArrayList<>(numOutputs);
			for (int i = 0; i < numOutputs; i++) {
				outputs.add(outSerializer.deserialize(inputView).<OUT>asRecord().getValue());
			}
			deserialized.setOutputCollection(outputs);
		}

		return deserialized;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && o instanceof InFlightElement) {
			InFlightElement that = (InFlightElement) o;
			return this.element.equals(that.element) &&
				(this.outputs == null ? that.outputs == null : this.outputs.equals(that.outputs));
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = this.element.hashCode();
		return 31 * result + (this.outputs == null ? 0 : outputs.hashCode());
	}
}
