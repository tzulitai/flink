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
 * Utility methods for {@link TypeSerializerBuilder}.
 */
@Internal
public class TypeSerializerBuilderUtils {

	/**
	 * Creates an array of serializers with the provided array of serializer builders.
	 */
	public static TypeSerializer<?>[] buildSerializers(TypeSerializerBuilder<?>[] builders) {
		final TypeSerializer<?>[] serializers = new TypeSerializer<?>[builders.length];

		for (int i = 0; i < builders.length; i++) {
			serializers[i] = builders[i].build();
		}

		return serializers;
	}

	/**
	 * Creates an array of serializer builders with the provided array of serializers.
	 */
	public static TypeSerializerBuilder<?>[] createBuilders(TypeSerializer<?>[] serializers) {
		final TypeSerializerBuilder<?>[] builders = new TypeSerializerBuilder<?>[serializers.length];

		for (int i = 0; i < serializers.length; i++) {
			builders[i] = serializers[i].getBuilder();
		}

		return builders;
	}

	/**
	 * Writes a {@link TypeSerializerBuilder} to the provided data output view.
	 *
	 * <p>It is written with a format that allows it to be later read again using
	 * {@link #readSerializerBuilder(DataInputView, ClassLoader)}.
	 */
	public static void writeSerializerBuilder(
			DataOutputView out,
			TypeSerializerBuilder<?> builder) throws IOException {

		new TypeSerializerBuilderSerializationProxy<>(builder).write(out);
	}

	/**
	 * Reads from the provided data input view a {@link TypeSerializerBuilder}
	 * that was previously serialized using
	 * {@link #writeSerializerBuilder(DataOutputView, TypeSerializerBuilder)}.
	 */
	public static <T> TypeSerializerBuilder<T> readSerializerBuilder(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		final TypeSerializerBuilderSerializationProxy<T> proxy =
			new TypeSerializerBuilderSerializationProxy<>(userCodeClassLoader);
		proxy.read(in);

		return proxy.getSerializerBuilder();
	}

	/**
	 * Writes multiple {@link TypeSerializerBuilder}s to the provided data output view.
	 *
	 * <p>It is written with a format that allows it to be later read again using
	 * {@link #readSerializerBuilders(DataInputView, ClassLoader)}.
	 */
	public static void writeSerializerBuilders(
			DataOutputView out,
			TypeSerializerBuilder... builders) throws IOException {

		out.writeInt(builders.length);

		for (TypeSerializerBuilder<?> builder : builders) {
			new TypeSerializerBuilderSerializationProxy<>(builder).write(out);
		}
	}

	/**
	 * Reads from the provided data input view a {@link TypeSerializerBuilder}
	 * that was previously serialized using
	 * {@link #writeSerializerBuilder(DataOutputView, TypeSerializerBuilder)}.
	 */
	public static TypeSerializerBuilder<?>[] readSerializerBuilders(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		int numFields = in.readInt();
		final TypeSerializerBuilder<?>[] serializers = new TypeSerializerBuilder<?>[numFields];

		TypeSerializerBuilderSerializationProxy proxy;
		for (int i = 0; i < numFields; i++) {
			proxy = new TypeSerializerBuilderSerializationProxy<>(userCodeClassLoader);
			proxy.read(in);
			serializers[i] = proxy.getSerializerBuilder();
		}

		return serializers;
	}

	/**
	 * Serialization proxy for a {@link TypeSerializerBuilder}.
	 */
	@Internal
	public static class TypeSerializerBuilderSerializationProxy<T> extends VersionedIOReadableWritable {

		public static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerBuilder<T> serializerBuilder;

		public TypeSerializerBuilderSerializationProxy(TypeSerializerBuilder<T> serializerBuilder) {
			this.serializerBuilder = checkNotNull(serializerBuilder);
		}

		public TypeSerializerBuilderSerializationProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		}

		public TypeSerializerBuilder<T> getSerializerBuilder() {
			return serializerBuilder;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			InstantiationUtil.serializeObject(new DataOutputViewStream(out), serializerBuilder.getClass());
			serializerBuilder.write(out);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			Class<? extends TypeSerializerBuilder<T>> builderClass;
			try {
				builderClass = InstantiationUtil.deserializeObject(new DataInputViewStream(in), userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not find requested TypeSerializerBuilder class in classpath.", e);
			}

			serializerBuilder = InstantiationUtil.instantiate(builderClass);
			serializerBuilder.setUserCodeClassLoader(userCodeClassLoader);
			serializerBuilder.read(in);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
