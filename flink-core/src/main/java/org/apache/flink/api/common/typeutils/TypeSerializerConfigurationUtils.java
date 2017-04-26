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
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility methods for {@link TypeSerializerConfiguration}.
 */
@Internal
public class TypeSerializerConfigurationUtils {

	/**
	 * Creates an array of serializers with the provided array of serializer builders.
	 */
	public static TypeSerializer<?>[] buildSerializers(TypeSerializerConfiguration<?>[] builders) {
		final TypeSerializer<?>[] serializers = new TypeSerializer<?>[builders.length];

		for (int i = 0; i < builders.length; i++) {
			serializers[i] = builders[i].build();
		}

		return serializers;
	}

	/**
	 * Creates an array of serializer builders with the provided array of serializers.
	 */
	public static TypeSerializerConfiguration<?>[] createBuilders(TypeSerializer<?>[] serializers) {
		final TypeSerializerConfiguration<?>[] builders = new TypeSerializerConfiguration<?>[serializers.length];

		for (int i = 0; i < serializers.length; i++) {
			builders[i] = serializers[i].getConfiguration();
		}

		return builders;
	}

	/**
	 * Writes a {@link TypeSerializerConfiguration} to the provided data output view.
	 *
	 * <p>It is written with a format that allows it to be later read again using
	 * {@link #readSerializerBuilder(DataInputView, ClassLoader)}.
	 */
	public static void writeSerializerBuilder(
			DataOutputView out,
			TypeSerializerConfiguration<?> builder) throws IOException {

		new TypeSerializerConfigSerializationProxy<>(builder).write(out);
	}

	/**
	 * Reads from the provided data input view a {@link TypeSerializerConfiguration}
	 * that was previously serialized using
	 * {@link #writeSerializerBuilder(DataOutputView, TypeSerializerConfiguration)}.
	 */
	public static <T> TypeSerializerConfiguration<T> readSerializerBuilder(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		final TypeSerializerConfigSerializationProxy<T> proxy =
			new TypeSerializerConfigSerializationProxy<>(userCodeClassLoader);
		proxy.read(in);

		return proxy.getSerializerBuilder();
	}

	/**
	 * Writes multiple {@link TypeSerializerConfiguration}s to the provided data output view.
	 *
	 * <p>It is written with a format that allows it to be later read again using
	 * {@link #readSerializerBuilders(DataInputView, ClassLoader)}.
	 */
	public static void writeSerializerBuilders(
			DataOutputView out,
			TypeSerializerConfiguration... builders) throws IOException {

		out.writeInt(builders.length);

		for (TypeSerializerConfiguration<?> builder : builders) {
			new TypeSerializerConfigSerializationProxy<>(builder).write(out);
		}
	}

	/**
	 * Reads from the provided data input view a {@link TypeSerializerConfiguration}
	 * that was previously serialized using
	 * {@link #writeSerializerBuilder(DataOutputView, TypeSerializerConfiguration)}.
	 */
	public static TypeSerializerConfiguration<?>[] readSerializerBuilders(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		int numFields = in.readInt();
		final TypeSerializerConfiguration<?>[] serializers = new TypeSerializerConfiguration<?>[numFields];

		TypeSerializerConfigSerializationProxy proxy;
		for (int i = 0; i < numFields; i++) {
			proxy = new TypeSerializerConfigSerializationProxy<>(userCodeClassLoader);
			proxy.read(in);
			serializers[i] = proxy.getSerializerBuilder();
		}

		return serializers;
	}

	/**
	 * Serialization proxy for a {@link TypeSerializerConfiguration}.
	 */
	@Internal
	public static class TypeSerializerConfigSerializationProxy<T> extends VersionedIOReadableWritable {

		public static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerConfiguration<T> serializerConfig;

		public TypeSerializerConfigSerializationProxy(TypeSerializerConfiguration<T> serializerBuilder) {
			this.serializerConfig = checkNotNull(serializerBuilder);
		}

		public TypeSerializerConfigSerializationProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		}

		public TypeSerializerConfiguration<T> getSerializerBuilder() {
			return serializerConfig;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			InstantiationUtil.serializeObject(new DataOutputViewStream(out), serializerConfig.getClass());
			serializerConfig.write(out);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			Class<? extends TypeSerializerConfiguration<T>> builderClass;
			try {
				builderClass = InstantiationUtil.deserializeObject(new DataInputViewStream(in), userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not find requested TypeSerializerConfiguration class in classpath.", e);
			}

			serializerConfig = InstantiationUtil.instantiate(builderClass);
			serializerConfig.setUserCodeClassLoader(userCodeClassLoader);
			serializerConfig.read(in);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
