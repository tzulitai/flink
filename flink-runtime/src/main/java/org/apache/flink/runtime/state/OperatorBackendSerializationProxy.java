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

import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilderUtils;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.VersionMismatchException;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serialization proxy for all meta data in operator state backends. In the future we might also migrate the actual state
 * serialization logic here.
 */
public class OperatorBackendSerializationProxy extends VersionedIOReadableWritable {

	private static final int VERSION = 2;

	private List<DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<?>> namedStates;
	private ClassLoader userCodeClassLoader;

	private int restoredVersion;

	public OperatorBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
	}

	public OperatorBackendSerializationProxy(List<DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<?>> namedStates) {
		this.namedStates = checkNotNull(namedStates);
		Preconditions.checkArgument(namedStates.size() <= Short.MAX_VALUE);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	protected void resolveVersionRead(int foundVersion) throws VersionMismatchException {
		super.resolveVersionRead(foundVersion);
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

		out.writeShort(namedStates.size());
		for (DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<?> state : namedStates) {
			getNamedStateSerializationProxyForVersion(restoredVersion, state);
		}
	}

	@Override
	public void read(DataInputView out) throws IOException {
		super.read(out);

		int numKvStates = out.readShort();
		namedStates = new ArrayList<>(numKvStates);

		StateMetaInfoSerializationProxy<?> proxy;
		for (int i = 0; i < numKvStates; ++i) {
			proxy = getNamedStateSerializationProxyForVersion(restoredVersion, userCodeClassLoader);
			proxy.read(out);
			namedStates.add(proxy.getNamedState());
		}
	}

	public List<DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<?>> getNamedStates() {
		return namedStates;
	}

	//----------------------------------------------------------------------------------------------------------------------

	private StateMetaInfoSerializationProxy<?> getNamedStateSerializationProxyForVersion(
			int restoredVersion, DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<?> namedState) {

		switch (restoredVersion) {
			case 1:
				return new V1StateMetaInfoSerializationProxy<>(namedState);
			case VERSION:
				return new V2StateMetaInfoSerializationProxy<>(namedState);
			default:
				throw new RuntimeException();
		}
	}

	private StateMetaInfoSerializationProxy<?> getNamedStateSerializationProxyForVersion(
			int restoredVersion, ClassLoader userCodeClassLoader) {

		switch (restoredVersion) {
			case 1:
				return new V1StateMetaInfoSerializationProxy<>(userCodeClassLoader);
			case VERSION:
				return new V2StateMetaInfoSerializationProxy<>(userCodeClassLoader);
			default:
				throw new RuntimeException();
		}
	}

	private abstract class StateMetaInfoSerializationProxy<S> implements IOReadableWritable {

		protected ClassLoader userCodeClassLoader;
		protected DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<S> namedState;

		public StateMetaInfoSerializationProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		}

		public StateMetaInfoSerializationProxy(DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<S> namedState) {
			this.namedState = checkNotNull(namedState);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(namedState.getName());
			out.writeByte(namedState.getAssignmentMode().ordinal());
			TypeSerializerBuilderUtils.writeSerializerBuilder(out, namedState.getStateSerializerBuilder());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			namedState = new DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<>();
			namedState.setName(in.readUTF());
			namedState.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);
			namedState.setStateSerializerBuilder(readStateSerializerBuilder(in));
		}

		public DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<S> getNamedState() {
			return namedState;
		}

		protected abstract TypeSerializerBuilder<S> readStateSerializerBuilder(DataInputView in) throws IOException;
	}

	private class V1StateMetaInfoSerializationProxy<S> extends StateMetaInfoSerializationProxy<S> {

		public V1StateMetaInfoSerializationProxy(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		public V1StateMetaInfoSerializationProxy(DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<S> namedState) {
			super(namedState);
		}

		@Override
		protected TypeSerializerBuilder<S> readStateSerializerBuilder(DataInputView in) throws IOException {
			try {
				return InstantiationUtil.deserializeObject(new DataInputViewStream(in), userCodeClassLoader);
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			}
		}
	}

	public class V2StateMetaInfoSerializationProxy<S> extends StateMetaInfoSerializationProxy<S> {

		public V2StateMetaInfoSerializationProxy(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		public V2StateMetaInfoSerializationProxy(DefaultOperatorStateBackend.RegisteredOperatorStateMetaInfo<S> namedState) {
			super(namedState);
		}

		@Override
		protected TypeSerializerBuilder<S> readStateSerializerBuilder(DataInputView in) throws IOException {
			return TypeSerializerBuilderUtils.readSerializerBuilder(in, userCodeClassLoader);
		}
	}
}
