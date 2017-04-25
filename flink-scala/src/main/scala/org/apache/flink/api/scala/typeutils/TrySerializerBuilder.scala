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

import scala.util.Try

/**
  * Builder for [[TrySerializer]].
  */
@Internal
final class TrySerializerBuilder[A](
    private var elemSerializerBuilder: TypeSerializerBuilder[A],
    private var throwableSerializerBuilder: TypeSerializerBuilder[Throwable])
  extends TypeSerializerBuilder[Try[A]] {

  /** This empty nullary constructor is required for deserializing the builder. */
  def this() = this(null, null)

  override def write(out: DataOutputView): Unit = {
    super.write(out)
    TypeSerializerBuilderUtils.writeSerializerBuilder(out, elemSerializerBuilder)
    TypeSerializerBuilderUtils.writeSerializerBuilder(out, throwableSerializerBuilder)
  }

  override def read(in: DataInputView): Unit = {
    super.read(in)
    elemSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader)
    throwableSerializerBuilder = TypeSerializerBuilderUtils.readSerializerBuilder(in, getUserCodeClassLoader)
  }

  override def resolve(other: TypeSerializerBuilder[_]): Unit = {
    super.resolve(other)

    other match {
      case otheTrySerializerBuilder: TrySerializerBuilder[A] =>
        elemSerializerBuilder.resolve(otheTrySerializerBuilder.elemSerializerBuilder)
        throwableSerializerBuilder.resolve(otheTrySerializerBuilder.throwableSerializerBuilder)
      case _ =>
        throw new UnresolvableTypeSerializerBuilderException(
            "Cannot resolve this builder with another builder of type " + other.getClass)
    }
  }

  override def build(): TypeSerializer[Try[A]] = {
    new TrySerializer[A](elemSerializerBuilder.build(), throwableSerializerBuilder.build())
  }

  override def getVersion: Int = TrySerializerBuilder.VERSION
}

object TrySerializerBuilder {
  val VERSION = 1
}
