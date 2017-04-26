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
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base for builders of {@link TupleSerializerBase}. Subclasses should simply define
 * how the concrete {@link TupleSerializer} is built.
 *
 * @param <T> The type of the tuple that the created {@link TupleSerializerBase} handles.
 */
public abstract class TupleSerializerBuilderBase<T> extends TypeSerializerConfiguration<T> {

	private static final int VERSION = 1;

	private Class<T> tupleClass;
	private TypeSerializerConfiguration<?>[] fieldSerializerBuilders;

	/** This empty nullary constructor is required for deserializing the builder. */
	public TupleSerializerBuilderBase() {}

	public TupleSerializerBuilderBase(Class<T> tupleClass, TypeSerializerConfiguration<?>[] fieldSerializerBuilders) {
		this.tupleClass = checkNotNull(tupleClass);
		this.fieldSerializerBuilders = checkNotNull(fieldSerializerBuilders);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		InstantiationUtil.serializeObject(new DataOutputViewStream(out), tupleClass);
		TypeSerializerConfigurationUtils.writeSerializerBuilders(out, fieldSerializerBuilders);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		try {
			this.tupleClass = InstantiationUtil.deserializeObject(new DataInputViewStream(in), getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find class in classpath.", e);
		}

		this.fieldSerializerBuilders = TypeSerializerConfigurationUtils.readSerializerBuilders(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other.getClass() == this.getClass()) {
			if (tupleClass != ((TupleSerializerBuilderBase) other).tupleClass) {
				throw new UnresolvableTypeSerializerConfigurationException(
						"The tuple class cannot change. Was [" + tupleClass + "], " +
							"trying to resolve with [" + ((TupleSerializerBuilderBase) other).tupleClass + "]");
			}

			if (fieldSerializerBuilders.length != ((TupleSerializerBuilderBase) other).fieldSerializerBuilders.length) {
				throw new UnresolvableTypeSerializerConfigurationException(
						"The number of fields cannot change. Was " + fieldSerializerBuilders.length + ", " +
							"trying to resolve with " + ((TupleSerializerBuilderBase) other).fieldSerializerBuilders.length);
			} else {
				for (int i = 0; i < fieldSerializerBuilders.length; i++) {
					try {
						fieldSerializerBuilders[i].resolve(((TupleSerializerBuilderBase) other).fieldSerializerBuilders[i]);
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

	@Override
	public int getVersion() {
		return VERSION;
	}

	public Class<T> getTupleClass() {
		return tupleClass;
	}

	public TypeSerializerConfiguration<?>[] getFieldSerializerBuilders() {
		return fieldSerializerBuilders;
	}
}
