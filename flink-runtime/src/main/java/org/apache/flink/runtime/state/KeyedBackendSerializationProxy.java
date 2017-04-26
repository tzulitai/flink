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

import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.VersionMismatchException;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization proxy for all meta data in keyed state backends. In the future we might also migrate the actual state
 * serialization logic here.
 */
public class KeyedBackendSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 2;

	private TypeSerializerConfiguration<?> keySerializerBuilder;
	private List<RegisteredBackendStateMetaInfo<?, ?>> namedStates;

	private SerializationCompatibilityProxy versionProxy;
	private int restoredVersion;
	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(TypeSerializerConfiguration<?> keySerializerBuilder, List<RegisteredBackendStateMetaInfo<?, ?>> namedStates) {
		this.keySerializerBuilder = Preconditions.checkNotNull(keySerializerBuilder);
		this.restoredVersion = VERSION;

		Preconditions.checkNotNull(namedStates);
		Preconditions.checkArgument(namedStates.size() <= Short.MAX_VALUE);
		this.namedStates = namedStates;
	}

	public List<RegisteredBackendStateMetaInfo<?, ?>> getNamedStates() {
		return namedStates;
	}

	public TypeSerializerConfiguration<?> getKeySerializerBuilder() {
		return keySerializerBuilder;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	public int getRestoredVersion() {
		return restoredVersion;
	}

	@Override
	protected void resolveVersionRead(int foundVersion) throws VersionMismatchException {
		super.resolveVersionRead(foundVersion);
		this.versionProxy = getCompatibleVersion(foundVersion);
		this.restoredVersion = foundVersion;
	}

	@Override
	public boolean isCompatibleVersion(int version) {
		// we are compatible with version 2 (Flink 1.3.x) and version 1 (Flink 1.2.x)
		return super.isCompatibleVersion(version) || version == 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		if (versionProxy != null) {
			versionProxy.write(out);
		} else {
			getCompatibleVersion(getVersion()).write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		versionProxy.read(in);
	}

	//------------------------------------------------------------------------------------------------------

	private SerializationCompatibilityProxy getCompatibleVersion(int restoredVersion) {
		switch (restoredVersion) {
			case 1:
				return new V1SerializationCompatibilityProxy();
			case VERSION:
				return new V2SerializationCompatibilityProxy();
			default:
				throw new IllegalStateException("This should not happen; safeguard for future.");
		}
	}

	private abstract class SerializationCompatibilityProxy implements IOReadableWritable {
		@Override
		public void write(DataOutputView out) throws IOException {
			// --- write key serializer builder
			TypeSerializerConfigurationUtils.writeSerializerBuilder(out, keySerializerBuilder);

			// --- write state meta infos
			out.writeShort(namedStates.size());
			StateMetaInfoSerializationProxy<?, ?> stateMetaInfoProxy;
			for (RegisteredBackendStateMetaInfo<?, ?> kvState : namedStates) {
				stateMetaInfoProxy = new StateMetaInfoSerializationProxy<>(kvState);
				stateMetaInfoProxy.write(out);
			}
		}
	}

	private class V1SerializationCompatibilityProxy extends SerializationCompatibilityProxy {
		@Override
		public void read(DataInputView in) throws IOException {
			// in version 1, type serializers were directly written to state;
			// retrieve the serializer builder by first deserializing type serializers

			final TypeSerializerSerializationProxy<?> keySerializerProxy =
				new TypeSerializerSerializationProxy<>(userCodeClassLoader);
			keySerializerProxy.read(in);

			keySerializerBuilder = keySerializerProxy.getTypeSerializer().getConfiguration();

			// --- read state meta infos
			int numKvStates = in.readShort();
			namedStates = new ArrayList<>(numKvStates);

			StateMetaInfoSerializationProxy<?, ?> stateMetaInfoProxy;
			for (int i = 0; i < numKvStates; ++i) {
				stateMetaInfoProxy = new StateMetaInfoSerializationProxy<>(userCodeClassLoader);
				stateMetaInfoProxy.getVersionProxy(-1).read(in);
				namedStates.add(stateMetaInfoProxy.getStateMetaInfo());
			}
		}
	}

	private class V2SerializationCompatibilityProxy extends SerializationCompatibilityProxy {
		@Override
		public void read(DataInputView in) throws IOException {
			keySerializerBuilder = TypeSerializerConfigurationUtils.readSerializerBuilder(in, userCodeClassLoader);

			// --- read state meta infos
			int numKvStates = in.readShort();
			namedStates = new ArrayList<>(numKvStates);

			StateMetaInfoSerializationProxy<?, ?> stateMetaInfoProxy;
			for (int i = 0; i < numKvStates; ++i) {
				stateMetaInfoProxy = new StateMetaInfoSerializationProxy<>(userCodeClassLoader);
				stateMetaInfoProxy.read(in);
				namedStates.add(stateMetaInfoProxy.getStateMetaInfo());
			}
		}
	}
}
