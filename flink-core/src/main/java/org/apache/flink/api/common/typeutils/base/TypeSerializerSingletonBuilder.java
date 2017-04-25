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
import org.apache.flink.api.common.typeutils.BasicTypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Builder for {@link TypeSerializerSingleton}s.
 * This builder assumes that the serializer singleton's class has a static field named
 * "INSTANCE" referencing the singleton instance.
 *
 * @param <T> the type of the data handled by the serializer that this builder constructs.
 */
@Internal
public final class TypeSerializerSingletonBuilder<T> extends BasicTypeSerializerBuilder<T> {

	private static final int VERSION = 1;

	public TypeSerializerSingletonBuilder() {}

	public TypeSerializerSingletonBuilder(Class<? extends TypeSerializerSingleton<T>> serializerSingletonClass) {
		super(serializerSingletonClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializer<T> build() {
		try {
			return (TypeSerializer<T>) getSerializerClass().getField("INSTANCE").get(null);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Unable to build serializer singleton for class: " + getSerializerClass().getName(), e);
		}
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
