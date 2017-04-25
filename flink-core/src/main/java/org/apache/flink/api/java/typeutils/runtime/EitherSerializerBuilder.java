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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.api.common.typeutils.TypeSerializerBuilderUtils;
import org.apache.flink.api.common.typeutils.UnresolvableTypeSerializerBuilderException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Either;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for the {@link EitherSerializer}.
 *
 * @param <L> the Left value type that the created {@link EitherSerializer} handles.
 * @param <R> the Right value type that the created {@link EitherSerializer} handles.
 */
public class EitherSerializerBuilder<L, R> extends TypeSerializerBuilder<Either<L, R>> {

	private static final int VERSION = 1;

	private TypeSerializerBuilder<L> leftSerializerBuilder;
	private TypeSerializerBuilder<R> rightSerializerBuilder;

	/** This empty nullary constructor is required for deserializing the builder. */
	public EitherSerializerBuilder() {}

	public EitherSerializerBuilder(
			TypeSerializerBuilder<L> leftSerializerBuilder,
			TypeSerializerBuilder<R> rightSerializerBuilder) {

		this.leftSerializerBuilder = checkNotNull(leftSerializerBuilder);
		this.rightSerializerBuilder = checkNotNull(rightSerializerBuilder);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerBuilderUtils.writeSerializerBuilder(out, leftSerializerBuilder);
		TypeSerializerBuilderUtils.writeSerializerBuilder(out, rightSerializerBuilder);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		leftSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader());
		rightSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader());
	}

	@Override
	public void resolve(TypeSerializerBuilder<?> other) throws UnresolvableTypeSerializerBuilderException {
		super.resolve(other);

		if (other instanceof EitherSerializerBuilder) {
			leftSerializerBuilder.resolve(((EitherSerializerBuilder) other).leftSerializerBuilder);
			rightSerializerBuilder.resolve(((EitherSerializerBuilder) other).rightSerializerBuilder);
		} else {
			throw new UnresolvableTypeSerializerBuilderException(
					"Cannot resolve this builder with another builder of type " + other.getClass());
		}
	}

	@Override
	public TypeSerializer<Either<L, R>> build() {
		return new EitherSerializer<>(leftSerializerBuilder.build(), rightSerializerBuilder.build());
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
