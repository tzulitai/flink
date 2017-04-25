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


package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
  * Builder for [[EitherSerializer]].
  */
@Internal
final class EitherSerializerBuilder[A, B, T <: Either[A, B]](
    private var leftSerializerBuilder: TypeSerializerBuilder[A],
    private var rightSerializerBuilder: TypeSerializerBuilder[B])
  extends TypeSerializerBuilder[T] {

  /** This empty nullary constructor is required for deserializing the builder. */
  def this() = this(null, null)

  override def write(out: DataOutputView): Unit = {
    super.write(out)
    TypeSerializerBuilderUtils.writeSerializerBuilder(out, leftSerializerBuilder)
    TypeSerializerBuilderUtils.writeSerializerBuilder(out, rightSerializerBuilder)
  }

  override def read(in: DataInputView): Unit = {
    super.read(in)
    leftSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader)
    rightSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader)
  }

  @throws[UnresolvableTypeSerializerBuilderException]
  override def resolve(other: TypeSerializerBuilder[_]): Unit = {
    super.resolve(other)

    other match {
      case otherEitherSerializerBuilder: EitherSerializerBuilder[A, B, T] =>
        leftSerializerBuilder.resolve(otherEitherSerializerBuilder.leftSerializerBuilder)
        rightSerializerBuilder.resolve(otherEitherSerializerBuilder.leftSerializerBuilder)
      case _ =>
        throw new UnresolvableTypeSerializerBuilderException(
            "Cannot resolve this builder with another builder of type " + other.getClass)
    }
  }

  override def build(): TypeSerializer[T] = {
    new EitherSerializer[A, B, T](leftSerializerBuilder.build(), rightSerializerBuilder.build())
  }

  override def getVersion: Int = EitherSerializerBuilder.VERSION
}

object EitherSerializerBuilder {
  val VERSION = 1
}
