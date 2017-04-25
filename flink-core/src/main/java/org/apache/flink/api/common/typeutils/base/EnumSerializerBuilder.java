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
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for a {@link EnumSerializer}.
 *
 * @param <T> the enum type handled by the enum serializer that this builder constructs.
 */
@Internal
public class EnumSerializerBuilder<T extends Enum<T>> extends TypeSerializerBuilder<T> {

	private static final int VERSION = 1;

	private Class<T> enumClass;

	/**
	 * Since the enum serializer writes the ordinals of the enum constants for serialization,
	 * this builder maintains an array of the original enum constants.
	 */
	private T[] values;

	/** This empty nullary constructor is required for deserializing the builder. */
	public EnumSerializerBuilder() {}

	public EnumSerializerBuilder(Class<T> enumClass) {
		this.enumClass = checkNotNull(enumClass);
		this.values = enumClass.getEnumConstants();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
	}

	@Override
	public void resolve(TypeSerializerBuilder<?> other) throws UnresolvableTypeSerializerBuilderException {
		super.resolve(other);

		if (other instanceof EnumSerializerBuilder) {
			if (!enumClass.equals(((EnumSerializerBuilder) other).enumClass)) {
				throw new UnresolvableTypeSerializerBuilderException("Enum class cannot change for an EnumSerializer.");
			} else {
				for (int i = 0; i < values.length; i++) {
					if (values[i] != ((EnumSerializerBuilder) other).values[i]) {
						throw new UnresolvableTypeSerializerBuilderException(
								"New enum constants can only be appended to the enumeration.");
					}
				}
			}

			// the old enum constants are resolvable with the new ones; simply replace the constants array
			this.values = enumClass.getEnumConstants();
		} else {
			throw new UnresolvableTypeSerializerBuilderException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@Override
	public TypeSerializer<T> build() {
		return new EnumSerializer<>(enumClass);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
