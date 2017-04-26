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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link ListSerializer}.
 *
 * @param <T> The type of element that the created {@link ListSerializer} handles.
 */
public class ListSerializerBuilder<T> extends TypeSerializerConfiguration<List<T>> {

	private static final int VERSION = 1;

	private TypeSerializerConfiguration<T> elementSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public ListSerializerBuilder() {}

	public ListSerializerBuilder(TypeSerializerConfiguration<T> elementSerializerBuilder) {
		this.elementSerializerBuilder = checkNotNull(elementSerializerBuilder);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerConfigurationUtils.writeSerializerBuilder(out, elementSerializerBuilder);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		elementSerializerBuilder = TypeSerializerConfigurationUtils.readSerializerBuilder(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other instanceof ListSerializerBuilder) {
			elementSerializerBuilder.resolve(((ListSerializerBuilder) other).elementSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerConfigurationException();
		}
	}

	@Override
	public TypeSerializer<List<T>> build() {
		return new ListSerializer<>(elementSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
