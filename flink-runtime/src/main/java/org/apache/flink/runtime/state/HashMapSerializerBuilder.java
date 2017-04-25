/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilderUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;

/**
 * Builder for the {@link HashMapSerializer}.
 *
 * @param <K> The type of the map keys that the created {@link HashMapSerializer} serializes.
 * @param <V> The type of the map values that the created {@link HashMapSerializer} serializes.
 */
@Internal
public final class HashMapSerializerBuilder<K, V> extends TypeSerializerBuilder<HashMap<K, V>> {

	private static final int VERSION = 1;

	private TypeSerializerBuilder<K> keySerializerBuilder;
	private TypeSerializerBuilder<V> valueSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public HashMapSerializerBuilder() {}

	public HashMapSerializerBuilder(
			TypeSerializerBuilder<K> keySerializerBuilder,
			TypeSerializerBuilder<V> valueSerializerBuilder) {

		this.keySerializerBuilder = Preconditions.checkNotNull(keySerializerBuilder);
		this.valueSerializerBuilder = Preconditions.checkNotNull(valueSerializerBuilder);
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
	public void resolve(TypeSerializerBuilder<?> other) throws UnresolvableTypeSerializerBuilderException {
		super.resolve(other);

		if (other instanceof HashMapSerializerBuilder) {
			keySerializerBuilder.resolve(((HashMapSerializerBuilder) other).keySerializerBuilder);
			valueSerializerBuilder.resolve(((HashMapSerializerBuilder) other).valueSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerBuilderException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@Override
	public TypeSerializer<HashMap<K, V>> build() {
		return new HashMapSerializer<>(keySerializerBuilder.build(), valueSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
