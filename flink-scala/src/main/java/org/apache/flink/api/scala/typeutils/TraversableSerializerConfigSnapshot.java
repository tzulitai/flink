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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import scala.collection.TraversableOnce;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.List$;

import java.io.IOException;

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

	private T collectionClass;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public TraversableSerializerConfigSnapshot() {}

	public TraversableSerializerConfigSnapshot(
			T collectionClass,
			TypeSerializer<E> elementSerializer) {

		super(elementSerializer);
		this.collectionClass = collectionClass;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		out.writeUTF(collectionClass.toString());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		if (getReadVersion() >= 2) {
			String cbfClassname = in.readUTF();

			try {
				//this.collectionClass = (Class<T>) Class.forName(cbfClassname, true, getUserCodeClassLoader());
			} catch (Exception e) {
				throw new RuntimeException("Oops", e);
			}
		}
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	protected boolean containsSerializers() {
		return getReadVersion() < 2;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<T> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new TraversableSerializer<T, E>(collectionClass, (TypeSerializer<E>) restoredNestedSerializers[0]) {

			private static final long serialVersionUID = -4553874422526425327L;

			@Override
			public CanBuildFrom<T, E, T> getCbf() {
				return collectionClass.getClass().OnceCanBuildFrom();
			}
		};
	}
}
