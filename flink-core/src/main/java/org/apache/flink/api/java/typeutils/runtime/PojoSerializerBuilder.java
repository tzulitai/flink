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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerConfigurationException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link PojoSerializer}.
 *
 * <p>NOTE: Currently, this builder does not guarantee that the created serializer
 * is compatible for reading serialized POJOs that are subclasses of the
 * POJO class.
 *
 * @param <T> The POJO type that the created {@link PojoSerializer} handles.
 */
@Internal
public final class PojoSerializerBuilder<T> extends TypeSerializerConfiguration<T> {

	private static final int VERSION = 1;

	private Class<T> pojoClass;
	private TypeSerializerConfiguration<?>[] fieldSerializerBuilders;

	private LinkedHashMap<Class<?>, Integer> registeredClasses;
	private TypeSerializerConfiguration<?>[] registeredSerializerBuilders;

	private Field[] fields;
	private ExecutionConfig executionConfig;

	private HashMap<Class<?>, TypeSerializerConfiguration<?>> subclassSerializerBuilders;

	/** This empty nullary constructor is required for deserializing the builder. */
	public PojoSerializerBuilder() {}

	public PojoSerializerBuilder(
			Class<T> pojoClass,
			TypeSerializerConfiguration<?>[] fieldSerializerBuilders,
			LinkedHashMap<Class<?>, Integer> registeredClasses,
			TypeSerializerConfiguration<?>[] registeredSerializerBuilders,
			Field[] fields,
			ExecutionConfig executionConfig,
			HashMap<Class<?>, TypeSerializerConfiguration<?>> subclassSerializerBuilders) {

		this.pojoClass = checkNotNull(pojoClass);
		this.fieldSerializerBuilders = checkNotNull(fieldSerializerBuilders);
		this.registeredClasses = checkNotNull(registeredClasses);
		this.registeredSerializerBuilders = checkNotNull(registeredSerializerBuilders);
		this.fields = checkNotNull(fields);
		this.executionConfig = checkNotNull(executionConfig);
		this.subclassSerializerBuilders = checkNotNull(subclassSerializerBuilders);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out);

		InstantiationUtil.serializeObject(outViewWrapper, pojoClass);
		TypeSerializerConfigurationUtils.writeSerializerBuilders(out, fieldSerializerBuilders);

		out.writeInt(registeredClasses.size());
		for (Map.Entry<Class<?>, Integer> entry : registeredClasses.entrySet()) {
			InstantiationUtil.serializeObject(outViewWrapper, entry.getKey());
			out.writeInt(entry.getValue());
		}

		TypeSerializerConfigurationUtils.writeSerializerBuilders(out, registeredSerializerBuilders);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		final DataInputViewStream inViewWrapper = new DataInputViewStream(in);

		try {
			pojoClass = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested class in classpath.", e);
		}

		fieldSerializerBuilders = TypeSerializerConfigurationUtils.readSerializerBuilders(in, getUserCodeClassLoader());

		int numRegisteredClasses = in.readInt();
		registeredClasses = new LinkedHashMap<>(numRegisteredClasses);
		try {
			for (int i = 0; i < numRegisteredClasses; i++) {
				Class<?> registeredClass = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
				int tag = in.readInt();
				registeredClasses.put(registeredClass, tag);
			}
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested class in classpath.", e);
		}
	}

	@Override
	public void resolve(TypeSerializerConfiguration<?> other) throws UnresolvableTypeSerializerConfigurationException {
		super.resolve(other);

		if (other instanceof PojoSerializerBuilder) {
			if (pojoClass != ((PojoSerializerBuilder) other).pojoClass) {
				throw new UnresolvableTypeSerializerConfigurationException("Cannot resolve different POJO classes. " +
						"Was " + pojoClass.getName() + ", trying to resolve with " + other.getClass().getName());
			}

			if (fieldSerializerBuilders.length != ((PojoSerializerBuilder) other).fieldSerializerBuilders.length) {
				throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve POJO serializer builders with different number of fields. " +
						"Had " + fieldSerializerBuilders.length + " fields, " +
						"trying to resolve with " + ((PojoSerializerBuilder) other).fieldSerializerBuilders.length + " fields.");
			}

			for (int i = 0; i < fieldSerializerBuilders.length; i++) {
				fieldSerializerBuilders[i].resolve(((PojoSerializerBuilder) other).fieldSerializerBuilders[i]);
			}

			for ()
		} else {
			throw new UnresolvableTypeSerializerConfigurationException(
					"Cannot resolve different TypeSerializerConfiguration types. " +
						"Was " + this.getClass().getName() + ", trying to resolve with " + other.getClass().getName());
		}
	}

	@Override
	public TypeSerializer<T> build() {
		return null;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
