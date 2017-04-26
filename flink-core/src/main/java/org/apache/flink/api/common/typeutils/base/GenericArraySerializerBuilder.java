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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link GenericArraySerializer}.
 *
 * @param <C> The component type that the created {@link GenericArraySerializer} handles.
 */
@Internal
public class GenericArraySerializerBuilder<C> extends TypeSerializerConfiguration<C[]> {

	private static final int VERSION = 1;

	private Class<C> componentClass;
	private TypeSerializerConfiguration<C> componentSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public GenericArraySerializerBuilder() {}

	@SuppressWarnings("unchecked")
	public GenericArraySerializerBuilder(Class<C> componentClass, TypeSerializerConfiguration<C> componentSerializerBuilder) {
		this.componentClass = checkNotNull(componentClass);
		this.componentSerializerBuilder = checkNotNull(componentSerializerBuilder);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		InstantiationUtil.serializeObject(new DataOutputViewStream(out), componentClass);

		TypeSerializerConfigurationUtils.writeSerializerBuilder(out, componentSerializerBuilder);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		try {
			InstantiationUtil.deserializeObject(new DataInputViewStream(in), getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested class in classpath.", e);
		}

		componentSerializerBuilder = TypeSerializerConfigurationUtils.readSerializerBuilder(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) {
		super.resolve(other);

		if (other instanceof GenericArraySerializerBuilder) {
			if (componentClass != ((GenericArraySerializerBuilder) other).componentClass) {
				throw new UnresolvableTypeSerializerConfigurationException("Class of array components cannot change.");
			}

			componentSerializerBuilder.resolve(((GenericArraySerializerBuilder) other).componentSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public GenericArraySerializer<C> build() {
		return new GenericArraySerializer<>(componentClass, componentSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
