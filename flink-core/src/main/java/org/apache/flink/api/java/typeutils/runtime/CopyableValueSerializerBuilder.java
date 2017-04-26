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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link CopyableValueSerializer}.
 *
 * @param <T> The copyable value type that the created {@link CopyableValueSerializer} handles.
 */
public class CopyableValueSerializerBuilder<T extends CopyableValue<T>> extends TypeSerializerConfiguration<T> {

	private static final int VERSION = 1;

	private Class<T> valueClass;

	/**
	 * This empty nullary constructor is required for deserializing the builder.
	 */
	public CopyableValueSerializerBuilder() {
	}

	public CopyableValueSerializerBuilder(Class<T> valueClass) {
		this.valueClass = checkNotNull(valueClass);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		InstantiationUtil.serializeObject(new DataOutputViewStream(out), valueClass);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		try {
			InstantiationUtil.deserializeObject(new DataInputViewStream(in), getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested class in classpath.", e);
		}
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other instanceof CopyableValueSerializerBuilder) {
			if (valueClass != ((CopyableValueSerializerBuilder) other).valueClass) {
				throw new UnresolvableTypeSerializerConfigurationException(
					"The class of the copyable value cannot change. Was [" + valueClass + "], " +
						"trying to resolve with [" + ((CopyableValueSerializerBuilder) other).valueClass + "]");
			}
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
				"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@Override
	public TypeSerializer<T> build() {
		return new CopyableValueSerializer<>(valueClass);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
