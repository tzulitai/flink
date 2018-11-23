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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ComplexTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Either;

/**
 * Configuration snapshot for the {@link EitherSerializer}.
 */
@Internal
public final class EitherSerializerSnapshot<L, R> extends ComplexTypeSerializerSnapshot<Either<L, R>> {

	private static final int CURRENT_VERSION = 2;

	/**
	 * Constructor for read instantiation.
	 */
	@SuppressWarnings({"unused", "unchecked"})
	public EitherSerializerSnapshot() {
		super(
			EitherSerializer.class,
			nestedSerializers -> new EitherSerializer<>((TypeSerializer<L>) nestedSerializers[0], (TypeSerializer<R>) nestedSerializers[1])
		);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public EitherSerializerSnapshot(EitherSerializer<L, R> eitherSerializer) {
		super(eitherSerializer);
	}

	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}
}
