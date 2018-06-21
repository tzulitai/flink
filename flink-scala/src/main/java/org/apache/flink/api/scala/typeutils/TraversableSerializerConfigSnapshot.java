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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;

import scala.collection.TraversableOnce;
import scala.collection.generic.CanBuildFrom;
import scala.collection.mutable.Builder;

/**
 * A {@link TypeSerializerConfigSnapshot} for the Scala {@link TraversableSerializer}.
 *
 * <p>This configuration snapshot class is implemented in Java because Scala does not
 * allow calling different base class constructors from subclasses, while we need that
 * for the default empty constructor.
 */
public class TraversableSerializerConfigSnapshot<T extends TraversableOnce<E>, E>
		extends CompositeTypeSerializerConfigSnapshot<T> {

	private static final int VERSION = 2;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public TraversableSerializerConfigSnapshot() {}

	public TraversableSerializerConfigSnapshot(TypeSerializer<E> elementSerializer) {
		super(elementSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	protected boolean containsSerializers() {
		return getReadVersion() < 2;
	}

	@Override
	protected TypeSerializer<T> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new TraversableSerializer<T, E>((TypeSerializer<E>) restoredNestedSerializers[0]) {

			private static final long serialVersionUID = -4553874422526425327L;

			@Override
			public CanBuildFrom<T, E, T> getCbf() {
				return new CanBuildFrom<T, E, T>() {
					@Override
					public Builder<E, T> apply() {
						return null;
					}

					@Override
					public Builder<E, T> apply(T t) {
						return null;
					}};
			}};
	}
}
