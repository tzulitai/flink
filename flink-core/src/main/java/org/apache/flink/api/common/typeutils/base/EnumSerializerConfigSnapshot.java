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
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Configuration snapshot of a serializer for enumerations.
 *
 * Configuration contains the enum class, and an array of the enum's constants
 * that existed when the configuration snapshot was taken.
 *
 * @param <T> the enum type.
 */
@Internal
public final class EnumSerializerConfigSnapshot<T extends Enum<T>> extends TypeSerializerConfigSnapshot {

	private static final int VERSION = 1;

	private Class<T> enumClass;
	private T[] enumConstants;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public EnumSerializerConfigSnapshot() {}

	public EnumSerializerConfigSnapshot(Class<T> enumClass) {
		this.enumClass = Preconditions.checkNotNull(enumClass);
		this.enumConstants = enumClass.getEnumConstants();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
			InstantiationUtil.serializeObject(outViewWrapper, enumClass);
			InstantiationUtil.serializeObject(outViewWrapper, enumConstants);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
			try {
				enumClass = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
				enumConstants = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IOException("The requested enum class cannot be found in classpath.", e);
			}
		}
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	public Class<T> getEnumClass() {
		return enumClass;
	}

	public T[] getEnumConstants() {
		return enumConstants;
	}
}
