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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 *
 * @param <T>
 */
public final class KryoSerializerBuilder<T> extends TypeSerializerBuilder<T> {

	private final static int VERSION = 1; // format

	/** */
	private Class<T> type;

	/** */
	private LinkedHashMap<String, DefaultSerializer> defaultSerializers;

	/** Registrations, in the exact order that they should be inflicted. */
	private LinkedHashMap<String, RegisteredSerializer> registrations;

	public KryoSerializerBuilder() {}

	@SuppressWarnings("unchecked")
	public KryoSerializerBuilder(
			Class<T> type,
			LinkedHashMap<String, DefaultSerializer> defaultSerializers,
			LinkedHashMap<String, RegisteredSerializer> registrations) {

		this.type = checkNotNull(type);
		this.defaultSerializers = checkNotNull(defaultSerializers);
		this.registrations = checkNotNull(registrations);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out);

		InstantiationUtil.serializeObject(outViewWrapper, type);

		out.write(defaultSerializers.size());
		for (Map.Entry<String, DefaultSerializer> defaultSerializerEntry : defaultSerializers.entrySet()) {
			out.writeUTF(defaultSerializerEntry.getKey());

			final DefaultSerializer.SerializationProxy proxy =
				new DefaultSerializer.SerializationProxy(defaultSerializerEntry.getValue());
			proxy.write(out);
		}

		out.write(registrations.size());
		for (Map.Entry<String, RegisteredSerializer> registeredSerializerEntry : registrations.entrySet()) {
			out.writeUTF(registeredSerializerEntry.getKey());

			final RegisteredSerializer.SerializationProxy proxy =
				new RegisteredSerializer.SerializationProxy(registeredSerializerEntry.getValue());
			proxy.write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		final DataInputViewStream inViewStream = new DataInputViewStream(in);

		try {
			InstantiationUtil.deserializeObject(inViewStream, getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {

		}

		int numDefaultSerializers = in.readInt();
		defaultSerializers = new LinkedHashMap<>(numDefaultSerializers);
		for (int i = 0; i < numDefaultSerializers; i++) {
			String className = in.readUTF();
			try {
				Class.forName(className, false, getUserCodeClassLoader());
			} catch (ClassNotFoundException e) {
				className = DummyRegistrationUtils.getUniqueDummyRegisteredClassIdentifier();
			}

			final DefaultSerializer.SerializationProxy proxy =
				new DefaultSerializer.SerializationProxy(getUserCodeClassLoader());
			proxy.read(in);
			defaultSerializers.put(className, proxy.getDefaultSerializer());
		}

		int numRegistrations = in.readInt();
		registrations = new LinkedHashMap<>(numRegistrations);
		for (int i = 0; i < numRegistrations; i++) {
			String className = in.readUTF();

			final RegisteredSerializer.SerializationProxy proxy =
				new RegisteredSerializer.SerializationProxy(getUserCodeClassLoader());
			proxy.read(in);
		}
	}

	@Override
	public void resolve(TypeSerializerBuilder<?> other) throws UnresolvableTypeSerializerBuilderException {
		super.resolve(other);

		if (other instanceof KryoSerializerBuilder) {
			KryoSerializerBuilder<?> otherKryoSerializerBuilder = (KryoSerializerBuilder) other;

			if (type != otherKryoSerializerBuilder.type) {
				throw new UnresolvableTypeSerializerBuilderException();
			}
			this.registrations.putAll(otherKryoSerializerBuilder.registrations);
			this.defaultSerializers = otherKryoSerializerBuilder.defaultSerializers;
		} else {
			throw new UnresolvableTypeSerializerBuilderException();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public KryoSerializer<T> build() {
		return new KryoSerializer<>(type, defaultSerializers, registrations);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
