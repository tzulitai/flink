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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Type Serializer Builder contains the configuration of a {@link TypeSerializer},
 * such that it can be used to rebuild the type serializer to the exact same state when
 * the builder was retrieved using {@link TypeSerializer#getConfiguration()}.
 *
 * <p>Note that the terms "builder configuration" and "serializer configuration / state" is
 * interchangeable, as generally the builder's configuration is an extraction of a serializer's
 * current configuration or state.
 *
 * <p>Builders can be serialized to binary representations and vice-versa. The representation
 * contains information about the builder's configuration.
 *
 * <h2>Serialization Compatibility</h2>
 *
 * Type Serializer Builders are the means to maintain serialization compatibility for
 * old data written by a serializer that was later upgraded and contains different
 * configuration.
 *
 * This is done by resolving the previous builder (with old configuration) with a new builder
 * (with the new configuration). The resolved builder should be reconfigured such that
 * the serializer it builds is capable of reading old data using the new configuration.
 *
 * <h2>Implementation Limitations</h2>
 *
 * Subclass implementations must have an empty nullary constructor, as this builder
 * class also serves as a deserialization proxy to read serialized builders.
 *
 * @param <T> the type of the data handled by the serializer that this builder constructs.
 */
@PublicEvolving
public abstract class TypeSerializerConfiguration<T> extends VersionedIOReadableWritable {

	/**
	 * The user code class loader.
	 *
	 * <p>There would be a user code class loader only if this builder
	 * instance was deserialized from its binary representation.
	 */
	private ClassLoader userCodeClassLoader;

	/**
	 * Create an empty builder instance that serves as the
	 * deserialization proxy for previous serialized builders.
	 *
	 * <p>After creating this proxy, use {@link TypeSerializerConfiguration#read(DataInputView)}
	 * to deserialize the actual configuration.
	 */
	public TypeSerializerConfiguration() {}

	/**
	 * Set the user code class loader. This should only be relevant when
	 * the builder instance is used as a deserialization proxy.
	 *
	 * @param userCodeClassLoader the user code class loader.
	 */
	public void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
	}

	// ------------------------------------------------------------------------------------------
	//  Base De-/Serialization Proxy Methods
	//
	//  Any change to these methods that results in a different serialization format must
	//  be reflected with a version uptick in all implementing subclasses.
	// ------------------------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
	}

	/**
	 * Resolve this configuration with another configuration.
	 *
	 * <p>This builder can be successfully resolved with the other builder,
	 * if this builder can be reconfigured such that the new serializer it builds
	 * is capable of reading old data written by a serializer
	 *
	 * <p>Subclasses should extend this method to their specific needs.
	 *
	 * @param other the other builder to resolve with.
	 *
	 * @throws UnresolvableTypeSerializerConfigurationException if this builder cannot be resolved with the other builder
	 */
	@SuppressWarnings("unchecked")
	public abstract TypeSerializerConfiguration<?> resolve(TypeSerializerConfiguration<?> other)
			throws UnresolvableTypeSerializerConfigurationException;

	//
	public abstract TypeSerializerFactory getFactory();

	//
	public abstract TypeSerializer<T> createSerializer();

	//
	public abstract TypeSerializer<T> createSerializer(TypeSerializerConfiguration<?> other);

	//
	public abstract TypeSerializer<T> restoreSerializer(TypeSerializerConfiguration<?> current)
			throws UnresolvableTypeSerializerConfigurationException;

	/**
	 * Get the user code class loader.
	 *
	 * <p>There would be a user code class loader only if this builder
	 * instance was deserialized from its binary representation.
	 * Otherwise, this would return {@code null}.
	 *
	 * @return the user code class loader, if this is a
	 * deserialized builder instance; {@code null} otherwise
	 */
	protected ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}

	/**
	 * Constructs a serializer with the current configuration of this builder.
	 *
	 * @return the constructed serializer.
	 */
	/*
	public abstract TypeSerializer<T> build();
	*/
}
