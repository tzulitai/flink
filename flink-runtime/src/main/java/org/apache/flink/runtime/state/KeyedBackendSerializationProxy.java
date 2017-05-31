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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization proxy for all meta data in keyed state backends. In the future we might also requiresMigration the actual state
 * serialization logic here.
 */
public class KeyedBackendSerializationProxy<K> extends VersionedIOReadableWritable {

	public static final int VERSION = 3;

	private TypeSerializer<K> keySerializer;
	private TypeSerializerConfigSnapshot keySerializerConfigSnapshot;

	private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots;

	private ClassLoader userCodeClassLoader;

	private boolean excludeSerializers;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(
			TypeSerializer<K> keySerializer,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots,
			boolean excludeSerializers) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializer.snapshotConfiguration());

		Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
		this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;

		this.excludeSerializers = excludeSerializers;
	}

	public List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		// we are compatible with version 3 (Flink 1.3.x) and version 1 & 2 (Flink 1.2.x)
		return new int[] {VERSION, 2, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeBoolean(excludeSerializers);

		Map<TypeSerializer<?>, Integer> serializerIndices = null;
		if (!excludeSerializers) {
			serializerIndices = buildSerializerIndices();
			TypeSerializerSerializationUtil.writeSerializerIndices(out, serializerIndices);
		}

		// write in a way to be fault tolerant of read failures when deserializing the key serializer
		TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
				out,
				Collections.singletonList(
					new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(keySerializer, keySerializerConfigSnapshot)),
				serializerIndices);

		// write individual registered keyed state metainfos
		out.writeShort(stateMetaInfoSnapshots.size());
		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> metaInfo : stateMetaInfoSnapshots) {
			KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(VERSION, metaInfo)
				.writeStateMetaInfo(out, serializerIndices);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		Map<Integer, TypeSerializer<?>> serializerIndex = null;
		// only starting from version 3, we have the key serializer and its config snapshot written
		if (getReadVersion() >= 3) {
			excludeSerializers = in.readBoolean();

			if (!excludeSerializers) {
				serializerIndex = TypeSerializerSerializationUtil.readSerializerIndex(in, userCodeClassLoader);
			}

			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> keySerializerAndConfig =
					TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
						in, userCodeClassLoader, serializerIndex).get(0);

			this.keySerializer = (TypeSerializer<K>) keySerializerAndConfig.f0;
			this.keySerializerConfigSnapshot = keySerializerAndConfig.f1;
		} else {
			excludeSerializers = false;

			this.keySerializer = TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader);
			this.keySerializerConfigSnapshot = null;
		}

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			stateMetaInfoSnapshots.add(
				KeyedBackendStateMetaInfoSnapshotReaderWriters
					.getReaderForVersion(getReadVersion(), userCodeClassLoader)
					.readStateMetaInfo(in, serializerIndex));
		}
	}

	private Map<TypeSerializer<?>, Integer> buildSerializerIndices() {
		int nextAvailableIndex = 0;

		// using reference equality for keys so that stateless
		// serializers are a single entry in the index
		final Map<TypeSerializer<?>, Integer> indices = new IdentityHashMap<>();

		indices.put(keySerializer, nextAvailableIndex++);

		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			if (!indices.containsKey(stateMetaInfoSnapshot.getNamespaceSerializer())) {
				indices.put(stateMetaInfoSnapshot.getNamespaceSerializer(), nextAvailableIndex++);
			}

			if (!indices.containsKey(stateMetaInfoSnapshot.getStateSerializer())) {
				indices.put(stateMetaInfoSnapshot.getStateSerializer(), nextAvailableIndex++);
			}
		}

		return indices;
	}
}
