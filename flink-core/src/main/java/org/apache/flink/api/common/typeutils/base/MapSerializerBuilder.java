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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilderUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link MapSerializer}.
 *
 * @param <K> The type of the map keys that the created {@link MapSerializer} serializes.
 * @param <V> The type of the map values that the created {@link MapSerializer} serializes.
 */
@Internal
public final class MapSerializerBuilder<K, V> extends TypeSerializerBuilder<Map<K, V>> {

	private static final int VERSION = 1;

	private TypeSerializerBuilder<K> keySerializerBuilder;
	private TypeSerializerBuilder<V> valueSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public MapSerializerBuilder() {}

	@SuppressWarnings("unchecked")
	public MapSerializerBuilder(TypeSerializerBuilder<K> keySerializerBuilder, TypeSerializerBuilder<V> valueSerializerBuilder) {

		this.keySerializerBuilder = checkNotNull(
				keySerializerBuilder,
				"The key serializer builder cannot be null");

		this.valueSerializerBuilder = checkNotNull(
				valueSerializerBuilder,
				"The value serializer builder cannot be null");
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerBuilderUtils.writeSerializerBuilder(out, keySerializerBuilder);
		TypeSerializerBuilderUtils.writeSerializerBuilder(out, valueSerializerBuilder);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		keySerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader());
		valueSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerBuilder<?> otherConfig) throws UnresolvableTypeSerializerBuilderException {
		super.resolve(otherConfig);

		if (otherConfig instanceof MapSerializerBuilder) {
			keySerializerBuilder.resolve(((MapSerializerBuilder) otherConfig).keySerializerBuilder);
			valueSerializerBuilder.resolve(((MapSerializerBuilder) otherConfig).valueSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerBuilderException();
		}
	}

	@Override
	public TypeSerializer<Map<K, V>> build() {
		return new MapSerializer<>(keySerializerBuilder.build(), valueSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
