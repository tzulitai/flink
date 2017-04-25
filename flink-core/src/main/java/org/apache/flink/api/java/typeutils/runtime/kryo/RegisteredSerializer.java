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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.VersionedIOReadableWritable;

public class RegisteredSerializer {
	private Class<?> typeClass;

	private Class<? extends Serializer<?>> serializerClass;
	private ExecutionConfig.SerializableSerializer<?> serializableSerializer;

	public RegisteredSerializer(Class<?> typeClass, Class<? extends Serializer<?>> serializerClass) {

	}

	public RegisteredSerializer(ExecutionConfig.SerializableSerializer<?> serializableSerializer) {

	}

	public Serializer<?> get(Kryo kryo) {
		if (serializerClass != null) {
			return ReflectionSerializerFactory.makeSerializer(kryo, serializerClass, typeClass);
		} else {
			return serializableSerializer.getSerializer();
		}
	}

	public static class SerializationProxy extends VersionedIOReadableWritable {

		private static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private RegisteredSerializer registeredSerializer;

		public SerializationProxy(ClassLoader userCodeClassLoader) {}

		public SerializationProxy(RegisteredSerializer registeredSerializer) {}

		public RegisteredSerializer getRegisteredSerializer() {
			return registeredSerializer;
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
