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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Basic configuration for stateless {@link TypeSerializer}s.
 *
 * <p>Resolving this configuration with another always results in the other configuration.
 *
 * <p>Custom serializers which use serialization frameworks that have their own built-in
 * serialization compatibility mechanisms such as <a href=>Thrift</a> or <a href=>Protobuf</a>
 * can simply use this configuration.
 *
 * @param <T>
 */
@PublicEvolving
public class BasicTypeSerializerConfiguration<T> extends TypeSerializerConfiguration<T> {

	private static final int VERSION = 1;

	private Class<? extends TypeSerializer<T>> serializerClass;

	/** This empty nullary constructor is required for deserializing the builder. */
	public BasicTypeSerializerConfiguration() {}

	public BasicTypeSerializerConfiguration(Class<? extends TypeSerializer<T>> serializerClass) {
		this.serializerClass = Preconditions.checkNotNull(serializerClass);
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
	public TypeSerializerConfiguration<?> resolve(TypeSerializerConfiguration<?> other)
			throws UnresolvableTypeSerializerConfigurationException {

		Preconditions.checkNotNull(other, "Cannot resolve with a null TypeSerializerConfiguration");

		if (this == other) {
			return this;
		}

		// since this configuration is for stateless serializers,
		// we can always assume that the other configuration is the resolution.
		return other;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	public Class<? extends TypeSerializer<T>> getSerializerClass() {
		return serializerClass;
	}
}
