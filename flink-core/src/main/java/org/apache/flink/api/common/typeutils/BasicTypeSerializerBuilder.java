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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple {@link TypeSerializerConfiguration} for configuration-less {@link TypeSerializer}s.
 * This builder assumes that the serializer to build has an empty nullary constructor (since
 * the serializer does not require any configuration).
 *
 * @param <T> the type of the data handled by the serializer that this builder constructs.
 */
@Internal
public class BasicTypeSerializerBuilder<T> extends TypeSerializerConfiguration<T> {

	private static final int VERSION = 1;

	private Class<? extends TypeSerializer<T>> serializerClass;

	/** This empty nullary constructor is required for deserializing the builder. */
	public BasicTypeSerializerBuilder() {}

	public BasicTypeSerializerBuilder(Class<? extends TypeSerializer<T>> serializerClass) {
		this.serializerClass = checkNotNull(serializerClass);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		InstantiationUtil.serializeObject(new DataOutputViewStream(out), serializerClass);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		try {
			serializerClass = InstantiationUtil.deserializeObject(new DataInputViewStream(in), getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested class in classpath.", e);
		}
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other instanceof BasicTypeSerializerBuilder) {
			if (serializerClass != ((BasicTypeSerializerBuilder) other).serializerClass) {
				throw new UnresolvableTypeSerializerConfigurationException(
					"The class of the serializer that the BasicTypeSerializerBuilder constructs cannot change");
			}
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@Override
	public TypeSerializer<T> build() {
		return InstantiationUtil.instantiate(serializerClass);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	protected Class<? extends TypeSerializer<T>> getSerializerClass() {
		return serializerClass;
	}
}
