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

import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link RowSerializer}.
 */
public class RowSerializerBuilder extends TypeSerializerConfiguration<Row> {

	private static final int VERSION = 1;

	private TypeSerializerConfiguration<?>[] fieldSerializerBuilders;

	/** This empty nullary constructor is required for deserializing the builder. */
	public RowSerializerBuilder() {}

	public RowSerializerBuilder(TypeSerializerConfiguration<?>[] fieldSerializerBuilders) {
		this.fieldSerializerBuilders = checkNotNull(fieldSerializerBuilders);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerConfigurationUtils.writeSerializerBuilders(out, fieldSerializerBuilders);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		this.fieldSerializerBuilders = TypeSerializerConfigurationUtils.readSerializerBuilders(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) {
		super.resolve(other);

		if (other instanceof RowSerializerBuilder) {
			if (fieldSerializerBuilders.length != ((RowSerializerBuilder) other).fieldSerializerBuilders.length) {
				throw new UnresolvableTypeSerializerConfigurationException(
						"The number of fields cannot change. Was " + fieldSerializerBuilders.length + ", " +
							"trying to resolve with " + ((RowSerializerBuilder) other).fieldSerializerBuilders.length);
			} else {
				for (int i = 0; i < fieldSerializerBuilders.length; i++) {
					try {
						fieldSerializerBuilders[i].resolve(((RowSerializerBuilder) other).fieldSerializerBuilders[i]);
					} catch (UnresolvableTypeSerializerConfigurationException e) {
						throw new UnresolvableTypeSerializerConfigurationException(
								"Serializer builder for field " + i + " could not be resolved", e);
					}
				}
			}
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public RowSerializer build() {
		return new RowSerializer(TypeSerializerConfigurationUtils.buildSerializers(fieldSerializerBuilders));
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
