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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link ArrayListSerializer}.
 *
 * @param <T> the array element type that the created {@link ArrayListSerializer} handles.
 */
@Internal
public final class ArrayListSerializerBuilder<T> extends TypeSerializerConfiguration<ArrayList<T>> {

	private static final int VERSION = 1;

	private TypeSerializerConfiguration<T> elementSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public ArrayListSerializerBuilder() {}

	@SuppressWarnings("unchecked")
	public ArrayListSerializerBuilder(TypeSerializerConfiguration<T> elementSerializerBuilder) {
		this.elementSerializerBuilder = checkNotNull(elementSerializerBuilder, "The element serializer cannot be null.");
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerConfigurationUtils.writeSerializerBuilder(out, elementSerializerBuilder);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		this.elementSerializerBuilder = TypeSerializerConfigurationUtils.readSerializerBuilder(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other instanceof ArrayListSerializerBuilder) {
			elementSerializerBuilder.resolve(((ArrayListSerializerBuilder) other).elementSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArrayListSerializer<T> build() {
		return new ArrayListSerializer<>(elementSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
