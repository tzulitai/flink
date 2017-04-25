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
import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all serializers and the
 * state name.
 *
 * @param <N> Type of namespace
 * @param <S> Type of state value
 */
public class RegisteredBackendStateMetaInfo<N, S> {

	private StateDescriptor.Type stateType;
	private String name;
	private TypeSerializerBuilder<N> namespaceSerializerBuilder;
	private TypeSerializerBuilder<S> stateSerializerBuilder;

	/** Empty constructor used by {@link StateMetaInfoSerializationProxy} when reading serialized meta info. */
	public RegisteredBackendStateMetaInfo() {}

	public RegisteredBackendStateMetaInfo(
			StateDescriptor.Type stateType,
			String name,
			TypeSerializerBuilder<N> namespaceSerializerBuilder,
			TypeSerializerBuilder<S> stateSerializerBuilder) {

		this.stateType = checkNotNull(stateType);
		this.name = checkNotNull(name);
		this.namespaceSerializerBuilder = checkNotNull(namespaceSerializerBuilder);
		this.stateSerializerBuilder = checkNotNull(stateSerializerBuilder);
	}

	public void setStateType(StateDescriptor.Type stateType) {
		this.stateType = stateType;
	}

	public StateDescriptor.Type getStateType() {
		return stateType;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setNamespaceSerializerBuilder(TypeSerializerBuilder<N> namespaceSerializerBuilder) {
		this.namespaceSerializerBuilder = namespaceSerializerBuilder;
	}

	public TypeSerializerBuilder<N> getNamespaceSerializerBuilder() {
		return namespaceSerializerBuilder;
	}

	public void setStateSerializerBuilder(TypeSerializerBuilder<S> stateSerializerBuilder) {
		this.stateSerializerBuilder = stateSerializerBuilder;
	}

	public TypeSerializerBuilder<S> getStateSerializerBuilder() {
		return stateSerializerBuilder;
	}

	public void resolve(RegisteredBackendStateMetaInfo<?, ?> other) throws UnresolvableStateException {

		if (this == other) {
			return;
		}

		if (null == other) {
			throw new UnresolvableStateException("Cannot resolve with an empty state meta info.");
		}

		if (stateType.equals(StateDescriptor.Type.UNKNOWN)) {
			throw new UnresolvableStateException("States of type " + StateDescriptor.Type.UNKNOWN + " cannot be resolved.");
		}

		if (other.stateType.equals(StateDescriptor.Type.UNKNOWN)) {
			throw new UnresolvableStateException("Cannot resolve with states of type " + StateDescriptor.Type.UNKNOWN);
		}

		if (!stateType.equals(other.stateType)) {
			throw new UnresolvableStateException("The state type cannot change. " +
					"Was " + stateType + ", trying to resolve with " + other.stateType);
		}

		if (!name.equals(other.getName())) {
			throw new UnresolvableStateException("The state name cannot change. " +
					"Was " + name + ", trying to resolve with " + other.getName());
		}

		try {
			namespaceSerializerBuilder.resolve(other.getNamespaceSerializerBuilder());
		} catch (UnresolvableTypeSerializerBuilderException e) {
			throw new UnresolvableStateException("Namespace serializer builder cannot be resolved. " +
					"Was " + namespaceSerializerBuilder + ", trying to resolve with " + other.getNamespaceSerializerBuilder(), e);
		}

		try {
			stateSerializerBuilder.resolve(other.getStateSerializerBuilder());
		} catch (UnresolvableTypeSerializerBuilderException e) {
			throw new UnresolvableStateException("State serializer builder cannot be resolved. " +
					"Was " + stateSerializerBuilder + ", trying to resolve with " + other.getStateSerializerBuilder(), e);
		}
	}

	/*
	public boolean canRestoreFrom(RegisteredBackendStateMetaInfo<?, ?> other) {

		if (this == other) {
			return true;
		}

		if (null == other) {
			return false;
		}

		if (!stateType.equals(StateDescriptor.Type.UNKNOWN)
				&& !other.stateType.equals(StateDescriptor.Type.UNKNOWN)
				&& !stateType.equals(other.stateType)) {
			return false;
		}

		if (!stateName.equals(other.getName())) {
			return false;
		}

		return (stateSerializer.canRestoreFrom(other.stateSerializer)) &&
				(namespaceSerializer.canRestoreFrom(other.namespaceSerializer)
						// we also check if there is just a migration proxy that should be replaced by any real serializer
						|| other.namespaceSerializer instanceof MigrationNamespaceSerializerProxy);
	}
	*/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RegisteredBackendStateMetaInfo<?, ?> that = (RegisteredBackendStateMetaInfo<?, ?>) o;

		if (!stateType.equals(that.stateType)) {
			return false;
		}

		if (!getName().equals(that.getName())) {
			return false;
		}

		return getStateSerializerBuilder().equals(that.getStateSerializerBuilder())
				&& getNamespaceSerializerBuilder().equals(that.getNamespaceSerializerBuilder());
	}

	@Override
	public String toString() {
		return "RegisteredBackendStateMetaInfo{" +
				"stateType=" + stateType +
				", name='" + name + '\'' +
				", namespaceSerializerBuilder=" + namespaceSerializerBuilder +
				", stateSerializerBuilder=" + stateSerializerBuilder +
				'}';
	}

	@Override
	public int hashCode() {
		int result = getName().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + (getNamespaceSerializerBuilder() != null ? getNamespaceSerializerBuilder().hashCode() : 0);
		result = 31 * result + (getStateSerializerBuilder() != null ? getStateSerializerBuilder().hashCode() : 0);
		return result;
	}
}
