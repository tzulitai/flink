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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.util.Preconditions;

/**
 * WIP.
 */
public abstract class StateSerializerProvider<T> {

	/**
	 * WIP.
	 */
	public abstract TypeSerializer<T> getStateSerializer();

	/**
	 * WIP.
	 */
	public static <T> StateSerializerProvider<T> from(TypeSerializer<T> serializerInstance) {
		return new InstanceStateSerializerProvider<>(serializerInstance);
	}

	/**
	 * WIP.
	 */
	public static <T> StateSerializerProvider<T> from(TypeSerializerSnapshot<T> serializerSnapshot) {
		return new SnapshotStateSerializerProvider<>(serializerSnapshot);
	}

	private static class InstanceStateSerializerProvider<T> extends StateSerializerProvider<T> {

		private final TypeSerializer<T> serializerInstance;

		InstanceStateSerializerProvider(TypeSerializer<T> serializerInstance) {
			this.serializerInstance = Preconditions.checkNotNull(serializerInstance);
		}

		@Override
		public TypeSerializer<T> getStateSerializer() {
			return serializerInstance;
		}
	}

	private static class SnapshotStateSerializerProvider<T> extends StateSerializerProvider<T> {

		private final TypeSerializerSnapshot<T> serializerSnapshot;

		SnapshotStateSerializerProvider(TypeSerializerSnapshot<T> serializerSnapshot) {
			this.serializerSnapshot = Preconditions.checkNotNull(serializerSnapshot);
		}

		@Override
		public TypeSerializer<T> getStateSerializer() {
			return serializerSnapshot.restoreSerializer();
		}
	}
}
