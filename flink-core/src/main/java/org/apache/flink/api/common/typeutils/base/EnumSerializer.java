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

import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ReconfigureResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public final class EnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> enumClass;

	private transient T[] values;

	public EnumSerializer(Class<T> enumClass) {
		this.enumClass = checkNotNull(enumClass);
		checkArgument(Enum.class.isAssignableFrom(enumClass), "not an enum");

		this.values = enumClass.getEnumConstants();
		checkArgument(this.values.length > 0, "cannot use an empty enum");
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public EnumSerializer<T> duplicate() {
		return this;
	}

	@Override
	public T createInstance() {
		return values[0];
	}

	@Override
	public T copy(T from) {
		return from;
	}

	@Override
	public T copy(T from, T reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		target.writeInt(record.ordinal());
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return values[source.readInt()];
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return values[source.readInt()];
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof EnumSerializer) {
			EnumSerializer<?> other = (EnumSerializer<?>) obj;

			return other.canEqual(this) && other.enumClass == this.enumClass;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof EnumSerializer;
	}

	@Override
	public int hashCode() {
		return enumClass.hashCode();
	}

	// --------------------------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.values = enumClass.getEnumConstants();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & reconfiguring
	// --------------------------------------------------------------------------------------------

	@Override
	public EnumSerializerConfigSnapshot snapshotConfiguration() {
		return new EnumSerializerConfigSnapshot<>(enumClass);
	}

	@Override
	public ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof EnumSerializerConfigSnapshot) {
			final EnumSerializerConfigSnapshot<?> config = (EnumSerializerConfigSnapshot) configSnapshot;

			if (enumClass == config.getEnumClass()) {
				// compatible only if new enum constants are only appended,
				// and original constants must be in the exact same order

				final T[] currentEnumConstants = enumClass.getEnumConstants();

				for (int i = 0; i < currentEnumConstants.length; i++) {
					if (currentEnumConstants[i] != config.getEnumConstants()[i]) {
						return ReconfigureResult.INCOMPATIBLE;
					}
				}

				return ReconfigureResult.COMPATIBLE;
			}
		}

		return ReconfigureResult.INCOMPATIBLE_DATA_TYPE;
	}
}
