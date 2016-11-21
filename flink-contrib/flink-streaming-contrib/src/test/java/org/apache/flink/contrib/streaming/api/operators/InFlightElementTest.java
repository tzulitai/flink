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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

public class InFlightElementTest {

	@Test
	public void testSerializeAndDeserialize() throws Exception {
		final StreamElementSerializer<String> inSerializer = new StreamElementSerializer<>(StringSerializer.INSTANCE);
		final StreamElementSerializer<Integer> outSerializer = new StreamElementSerializer<>(IntSerializer.INSTANCE);

		InFlightElement<Integer> recordWithoutOutputs = new InFlightElement<>(new StreamRecord<>("foo"));
		assertEquals(recordWithoutOutputs, serializeAndDeserialize(recordWithoutOutputs, inSerializer, outSerializer));

		InFlightElement<Integer> recordWithOutputs = new InFlightElement<>(new StreamRecord<>("foo"));
		Collection<Integer> outputs = new LinkedList<>();
		outputs.add(1);
		outputs.add(2);
		outputs.add(3);
		outputs.add(4);
		recordWithOutputs.setOutputCollection(outputs);
		assertEquals(recordWithOutputs, serializeAndDeserialize(recordWithOutputs, inSerializer, outSerializer));

		InFlightElement<Integer> watermark = new InFlightElement<>(new Watermark(123));
		assertEquals(watermark, serializeAndDeserialize(watermark, inSerializer, outSerializer));

		InFlightElement<Integer> latencyMarker = new InFlightElement<>(new LatencyMarker(123, 3, 5));
		assertEquals(latencyMarker, serializeAndDeserialize(latencyMarker, inSerializer, outSerializer));
	}

	private static <IN, OUT> InFlightElement<OUT> serializeAndDeserialize(
			InFlightElement<OUT> element,
			StreamElementSerializer<IN> inSerializer,
			StreamElementSerializer<OUT> outSerializer) throws IOException {

		DataOutputSerializer output = new DataOutputSerializer(32);
		InFlightElement.serialize(element, output, inSerializer, outSerializer);

		DataInputDeserializer input = new DataInputDeserializer(output.getByteArray(), 0, output.length());
		return InFlightElement.deserialize(input, inSerializer, outSerializer);
	}

}
