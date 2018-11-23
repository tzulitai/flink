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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * TODO can we allow this to also cover serializers which not only have nested serializers,
 * TODO but also some extra configuration around it? e.g. the GenericArraySerializer, which is configured with
 * TODO the class of the target type.
 */
public abstract class ComplexTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

	private CompositeSerializerSnapshot nestedSerializersSnapshot;

	private final Class<? extends TypeSerializer> expectedNewSerializerClass;
	private final ComplexSerializerFactory<T> factory;

	public ComplexTypeSerializerSnapshot(
			Class<? extends TypeSerializer> expectedNewSerializerClass,
			ComplexSerializerFactory<T> serializerFactory) {

		this.expectedNewSerializerClass = checkNotNull(expectedNewSerializerClass);
		this.factory = checkNotNull(serializerFactory);
	}

	public ComplexTypeSerializerSnapshot(TypeSerializer<T> serializer) {
		checkState(serializer instanceof ComplexTypeSerializer);

		this.expectedNewSerializerClass = serializer.getClass();
		this.nestedSerializersSnapshot = new CompositeSerializerSnapshot(
			checkNotNull(((ComplexTypeSerializer) serializer).getNestedSerializers())
		);

		// irrelevant fields for writing the snapshot
		this.factory = null;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedSerializersSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedSerializersSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		System.out.println(newSerializer.getClass());
		System.out.println(expectedNewSerializerClass);
		if (newSerializer.getClass().equals(expectedNewSerializerClass)) {
			checkState(newSerializer instanceof ComplexTypeSerializer);

			@SuppressWarnings("unchecked")
			ComplexTypeSerializer<T> complexTypeSerializer = (ComplexTypeSerializer<T>) newSerializer;

			return nestedSerializersSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				complexTypeSerializer.getNestedSerializers());
		} else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		return factory.create(nestedSerializersSnapshot.getRestoreSerializers());
	}

	protected interface ComplexSerializerFactory<T> {
		TypeSerializer<T> create(TypeSerializer<?>[] nestedSerializers);
	}
}
