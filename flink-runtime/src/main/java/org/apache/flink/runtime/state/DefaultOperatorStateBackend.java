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

import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

/**
 * Default implementation of OperatorStateStore that provides the ability to make snapshots.
 */
@Internal
public class DefaultOperatorStateBackend implements OperatorStateBackend {

	/** The default namespace for state in cases where no state name is provided */
	public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";
	
	private final Map<String, PartitionableListState<?>> registeredStates;
	private final CloseableRegistry closeStreamOnCancelRegistry;
	private final JavaSerializer<Serializable> javaSerializer;
	private final ClassLoader userClassloader;
	private final ExecutionConfig executionConfig;

	public DefaultOperatorStateBackend(ClassLoader userClassLoader, ExecutionConfig executionConfig) throws IOException {
		this.closeStreamOnCancelRegistry = new CloseableRegistry();
		this.userClassloader = Preconditions.checkNotNull(userClassLoader);
		this.executionConfig = executionConfig;
		this.javaSerializer = new JavaSerializer<>();
		this.registeredStates = new HashMap<>();
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return registeredStates.keySet();
	}

	@Override
	public void close() throws IOException {
		closeStreamOnCancelRegistry.close();
	}

	@Override
	public void dispose() {
		registeredStates.clear();
	}

	// -------------------------------------------------------------------------------------------
	//  State access methods
	// -------------------------------------------------------------------------------------------

	@Override
	public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
	}

	@Override
	public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.BROADCAST);
	}

	// -------------------------------------------------------------------------------------------
	//  Deprecated state access methods
	// -------------------------------------------------------------------------------------------

	/**
	 * @deprecated This was deprecated as part of a refinement to the function names.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@Deprecated
	@Override
	public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor);
	}

	/**
	 * @deprecated Using Java serialization for persisting state is not encouraged.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	@Override
	public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
		return (ListState<T>) getListState(new ListStateDescriptor<>(stateName, javaSerializer));
	}

	// -------------------------------------------------------------------------------------------
	//  Snapshot and restore
	// -------------------------------------------------------------------------------------------

	@Override
	public RunnableFuture<OperatorStateHandle> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

		if (registeredStates.isEmpty()) {
			return DoneFuture.nullValue();
		}

		List<RegisteredOperatorStateMetaInfo<?>> metaInfoList = new ArrayList<>(registeredStates.size());

		for (Map.Entry<String, PartitionableListState<?>> entry : registeredStates.entrySet()) {
			metaInfoList.add(entry.getValue().getStateMetaInfo());
		}

		Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData = new HashMap<>(registeredStates.size());

		CheckpointStreamFactory.CheckpointStateOutputStream out = streamFactory.
				createCheckpointStateOutputStream(checkpointId, timestamp);

		try {
			closeStreamOnCancelRegistry.registerClosable(out);

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			final OperatorBackendSerializationProxy backendSerializationProxy =
					new OperatorBackendSerializationProxy(metaInfoList);
			backendSerializationProxy.write(dov);

			dov.writeInt(registeredStates.size());
			for (Map.Entry<String, PartitionableListState<?>> entry : registeredStates.entrySet()) {
				PartitionableListState<?> value = entry.getValue();
				long[] partitionOffsets = value.writePartitions(out);
				OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
				writtenStatesMetaData.put(entry.getKey(), new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
			}

			OperatorStateHandle handle = new OperatorStateHandle(writtenStatesMetaData, out.closeAndGetHandle());

			return new DoneFuture<>(handle);
		} finally {
			closeStreamOnCancelRegistry.unregisterClosable(out);
			out.close();
		}
	}

	@Override
	public void restore(Collection<OperatorStateHandle> restoreSnapshots) throws Exception {

		if (null == restoreSnapshots) {
			return;
		}

		for (OperatorStateHandle stateHandle : restoreSnapshots) {

			if (stateHandle == null) {
				continue;
			}

			FSDataInputStream in = stateHandle.openInputStream();
			closeStreamOnCancelRegistry.registerClosable(in);

			ClassLoader restoreClassLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(userClassloader);
				OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(userClassloader);

				backendSerializationProxy.read(new DataInputViewStreamWrapper(in));

				List<RegisteredOperatorStateMetaInfo<?>> metaInfoList = backendSerializationProxy.getNamedStates();

				// Recreate all PartitionableListStates from the meta info
				for (RegisteredOperatorStateMetaInfo<?> restoredMetaInfo : metaInfoList) {
					if (!registeredStates.containsKey(restoredMetaInfo.getName())) {
						registeredStates.put(restoredMetaInfo.getName(), new PartitionableListState<>(restoredMetaInfo));
					} else {
						// this should never happen; adding as safe guard
						throw new IllegalStateException("Restored state meta info with " +
								"duplicate state name " + restoredMetaInfo.getName());
					}
				}

				// Restore all the state in PartitionableListStates
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> nameToOffsets :
						stateHandle.getStateNameToPartitionOffsets().entrySet()) {

					PartitionableListState<?> stateListForName = registeredStates.get(nameToOffsets.getKey());

					Preconditions.checkState(null != stateListForName, "Found state without " +
							"corresponding meta info: " + nameToOffsets.getKey());

					deserializeStateValues(stateListForName, in, nameToOffsets.getValue());
				}

			} finally {
				Thread.currentThread().setContextClassLoader(restoreClassLoader);
				closeStreamOnCancelRegistry.unregisterClosable(in);
				IOUtils.closeQuietly(in);
			}
		}
	}

	static final class RegisteredOperatorStateMetaInfo<S> {

		private String name;
		private TypeSerializerBuilder<S> stateSerializerBuilder;
		private OperatorStateHandle.Mode assignmentMode;

		/** Empty constructor used by when reading serialized meta info. */
		public RegisteredOperatorStateMetaInfo() {}

		public RegisteredOperatorStateMetaInfo(
				String name,
				TypeSerializerBuilder<S> stateSerializerBuilder,
				OperatorStateHandle.Mode assignmentMode) {

			this.name = Preconditions.checkNotNull(name);
			this.stateSerializerBuilder = Preconditions.checkNotNull(stateSerializerBuilder);
			this.assignmentMode = Preconditions.checkNotNull(assignmentMode);
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setStateSerializerBuilder(TypeSerializerBuilder<S> stateSerializerBuilder) {
			this.stateSerializerBuilder = stateSerializerBuilder;
		}

		public TypeSerializerBuilder<S> getStateSerializerBuilder() {
			return stateSerializerBuilder;
		}

		public void setAssignmentMode(OperatorStateHandle.Mode assignmentMode) {
			this.assignmentMode = assignmentMode;
		}

		public OperatorStateHandle.Mode getAssignmentMode() {
			return assignmentMode;
		}

		public void resolve(RegisteredOperatorStateMetaInfo<?> other) throws UnresolvableStateException {

			if (this == other) {
				return;
			}

			if (null == other) {
				throw new UnresolvableStateException("Cannot resolve with an empty state meta info.");
			}

			if (!name.equals(other.getName())) {
				throw new UnresolvableStateException("The state name cannot change. " +
						"Was " + name + ", trying to resolve with " + other.getName());
			}

			if (!assignmentMode.equals(other.getAssignmentMode())) {
				throw new UnresolvableStateException("The redistribution mode cannot change. " +
						"Was " + assignmentMode + ", trying to resolve with " + other.getAssignmentMode());
			}

			try {
				stateSerializerBuilder.resolve(other.getStateSerializerBuilder());
			} catch (UnresolvableTypeSerializerBuilderException e) {
				throw new UnresolvableStateException("State serializer builder cannot be resolved. " +
						"Was " + stateSerializerBuilder + ", trying to resolve with " + other.getStateSerializerBuilder(), e);
			}
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}

		@Override
		public int hashCode() {
			return super.hashCode();
		}

		@Override
		public String toString() {
			return "StateMetaInfo{" +
				"name='" + name + '\'' +
				", assignmentMode=" + assignmentMode +
				", stateSerializerBuilder=" + stateSerializerBuilder +
				'}';
		}
	}

	static final class PartitionableListState<S> implements ListState<S> {

		private final RegisteredOperatorStateMetaInfo<S> stateMetaInfo;
		private final List<S> internalList;

		public PartitionableListState(RegisteredOperatorStateMetaInfo<S> stateMetaInfo) {
			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
			this.internalList = new ArrayList<>();
		}

		@Override
		public void clear() {
			internalList.clear();
		}

		@Override
		public Iterable<S> get() {
			return internalList;
		}

		@Override
		public void add(S value) {
			internalList.add(value);
		}

		@Override
		public String toString() {
			return "PartitionableListState{" +
					"stateMetaInfo='" + stateMetaInfo + '\'' +
					", internalList=" + internalList +
					'}';
		}

		public RegisteredOperatorStateMetaInfo<S> getStateMetaInfo() {
			return stateMetaInfo;
		}

		public long[] writePartitions(FSDataOutputStream out) throws IOException {

			long[] partitionOffsets = new long[internalList.size()];
			TypeSerializer<S> partitionStateSerializer = stateMetaInfo.getStateSerializerBuilder().build();

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			for (int i = 0; i < internalList.size(); ++i) {
				S element = internalList.get(i);
				partitionOffsets[i] = out.getPos();
				partitionStateSerializer.serialize(element, dov);
			}

			return partitionOffsets;
		}
	}

	private <S> ListState<S> getListState(
		ListStateDescriptor<S> stateDescriptor,
		OperatorStateHandle.Mode mode) throws IOException {

		Preconditions.checkNotNull(stateDescriptor);

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		String name = Preconditions.checkNotNull(stateDescriptor.getName());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

		RegisteredOperatorStateMetaInfo<S> newStateMetaInfo =
				new RegisteredOperatorStateMetaInfo<>(name, partitionStateSerializer.getBuilder(), mode);

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredStates.get(name);

		if (null == partitionableListState) {
			registeredStates.put(name, new PartitionableListState<>(newStateMetaInfo));
		} else {
			partitionableListState.getStateMetaInfo().resolve(newStateMetaInfo);
		}

		return partitionableListState;
	}

	private static <S> void deserializeStateValues(
		PartitionableListState<S> stateListForName,
		FSDataInputStream in,
		OperatorStateHandle.StateMetaInfo metaInfo) throws IOException {

		if (null != metaInfo) {
			long[] offsets = metaInfo.getOffsets();
			if (null != offsets) {
				DataInputView div = new DataInputViewStreamWrapper(in);
				TypeSerializer<S> serializer = stateListForName.getStateMetaInfo().getStateSerializerBuilder().build();
				for (long offset : offsets) {
					in.seek(offset);
					stateListForName.add(serializer.deserialize(div));
				}
			}
		}
	}
}
