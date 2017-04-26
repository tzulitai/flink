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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigurationUtils;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.VersionMismatchException;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StateMetaInfoSerializationProxy<N, S> extends VersionedIOReadableWritable {

	private static final int VERSION = 1;

	private RegisteredBackendStateMetaInfo<N, S> stateMetaInfo;
	private ClassLoader userCodeClassLoader;

	private SerializationCompatibilityProxy versionProxy;

	public StateMetaInfoSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
	}

	public StateMetaInfoSerializationProxy(RegisteredBackendStateMetaInfo<N, S> stateMetaInfo) {
		this.stateMetaInfo = checkNotNull(stateMetaInfo);
	}

	public RegisteredBackendStateMetaInfo<N, S> getStateMetaInfo() {
		return stateMetaInfo;
	}

	@Override
	protected void resolveVersionRead(int foundVersion) throws VersionMismatchException {
		super.resolveVersionRead(foundVersion);
		this.versionProxy = getVersionProxy(foundVersion);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		if (versionProxy != null) {
			versionProxy.write(out);
		} else {
			getVersionProxy(getVersion()).write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		versionProxy.read(in);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	//------------------------------------------------------------------------------------------------------

	public SerializationCompatibilityProxy getVersionProxy(int restoredVersion) {
		switch (restoredVersion) {
			case -1:
				return new PreVersionedSerializationCompatilityProxy();
			case VERSION:
				return new V1SerializationCompatibilityProxy();
			default:
				throw new IllegalStateException("");
		}
	}

	public abstract class SerializationCompatibilityProxy implements IOReadableWritable {

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(stateMetaInfo.getStateType().ordinal());
			out.writeUTF(stateMetaInfo.getName());

			TypeSerializerConfigurationUtils.writeSerializerBuilder(out, stateMetaInfo.getNamespaceSerializerBuilder());
			TypeSerializerConfigurationUtils.writeSerializerBuilder(out, stateMetaInfo.getStateSerializerBuilder());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			stateMetaInfo = new RegisteredBackendStateMetaInfo<>();

			int enumOrdinal = in.readInt();
			stateMetaInfo.setStateType(StateDescriptor.Type.values()[enumOrdinal]);
			stateMetaInfo.setName(in.readUTF());

			stateMetaInfo.setNamespaceSerializerBuilder(readNamespaceSerializerBuilder(in, userCodeClassLoader));
			stateMetaInfo.setStateSerializerBuilder(readStateSerializerBuilder(in, userCodeClassLoader));
		}

		protected abstract TypeSerializerConfiguration<N> readNamespaceSerializerBuilder(
				DataInputView in,
				ClassLoader classLoader) throws IOException;

		protected abstract TypeSerializerConfiguration<S> readStateSerializerBuilder(
				DataInputView in,
				ClassLoader classLoader) throws IOException;
	}

	private class PreVersionedSerializationCompatilityProxy extends SerializationCompatibilityProxy {
		@Override
		protected TypeSerializerConfiguration<N> readNamespaceSerializerBuilder(
			DataInputView in,
			ClassLoader classLoader) throws IOException {

			final TypeSerializerSerializationProxy<N> namespaceSerializerProxy =
					new TypeSerializerSerializationProxy<>(classLoader);
			namespaceSerializerProxy.read(in);

			return namespaceSerializerProxy.getTypeSerializer().getConfiguration();
		}

		@Override
		protected TypeSerializerConfiguration<S> readStateSerializerBuilder(
			DataInputView in,
			ClassLoader classLoader) throws IOException {

			final TypeSerializerSerializationProxy<S> stateSerializerProxy =
					new TypeSerializerSerializationProxy<>(classLoader);
			stateSerializerProxy.read(in);

			return stateSerializerProxy.getTypeSerializer().getConfiguration();
		}
	}

	private class V1SerializationCompatibilityProxy extends SerializationCompatibilityProxy {
		@Override
		protected TypeSerializerConfiguration<N> readNamespaceSerializerBuilder(
			DataInputView in,
			ClassLoader classLoader) throws IOException {

			return TypeSerializerConfigurationUtils.readSerializerBuilder(in, userCodeClassLoader);
		}

		@Override
		protected TypeSerializerConfiguration<S> readStateSerializerBuilder(
			DataInputView in,
			ClassLoader classLoader) throws IOException {

			return TypeSerializerConfigurationUtils.readSerializerBuilder(in, userCodeClassLoader);
		}
	}
}
