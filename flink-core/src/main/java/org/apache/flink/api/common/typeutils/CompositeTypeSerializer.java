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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.util.Preconditions;

/**
 * Created by tzulitai on 18/06/2018.
 */
public abstract class CompositeTypeSerializer<T> extends TypeSerializer<T> {

	private final CompositeTypeSerializerConfigSnapshot<T> cachedConfigSnapshot;

	private final TypeSerializer<?>[] nestedSerializers;

	public CompositeTypeSerializer(
			CompositeTypeSerializerConfigSnapshot<T> cachedConfigSnapshot,
			TypeSerializer<?>... nestedSerializers) {

		this.cachedConfigSnapshot = Preconditions.checkNotNull(cachedConfigSnapshot);
		this.nestedSerializers = nestedSerializers;
	}

	@Override
	public TypeSerializerConfigSnapshot<T> snapshotConfiguration() {
		return cachedConfigSnapshot;
	}

	@Override
	protected CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		if (isComparableSnapshot(configSnapshot)) {
			@SuppressWarnings("unchecked")
			CompositeTypeSerializerConfigSnapshot<T> snapshot = (CompositeTypeSerializerConfigSnapshot<T>) configSnapshot;

			int i = 0;
			for (TypeSerializer<?> nestedSerializer : nestedSerializers) {
				CompatibilityResult<?> compatibilityResult = CompatibilityUtil.resolveCompatibilityResult(
					snapshot.getNestedSerializersAndConfigs().get(i).f1,
					nestedSerializer);

				if (compatibilityResult.isRequiresMigration()) {
					return CompatibilityResult.requiresMigration();
				}

				i++;
			}

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	protected boolean isComparableSnapshot(TypeSerializerConfigSnapshot<?> configSnapshot) {
		return configSnapshot.getClass().equals(cachedConfigSnapshot.getClass());
	}
}
