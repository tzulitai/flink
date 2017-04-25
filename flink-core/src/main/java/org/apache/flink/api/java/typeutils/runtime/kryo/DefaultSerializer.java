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

import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.VersionedIOReadableWritable;

public class DefaultSerializer {

	private Class<? extends Serializer<?>> serializerClass;
	private ExecutionConfig.SerializableSerializer<?> serializableSerializer;

	public DefaultSerializer(Class<? extends Serializer<?>> serializerClass) {

	}

	public DefaultSerializer(ExecutionConfig.SerializableSerializer<?> serializableSerializer) {

	}

	public boolean isSerializerClass() {
		return serializerClass != null;
	}

	public boolean isSerializerInstance() {
		return serializableSerializer != null;
	}

	public Class<? extends Serializer<?>> asSerializerClass() throws IllegalStateException {
		if (isSerializerClass()) {
			return serializerClass;
		} else {
			throw new IllegalStateException();
		}
	}

	public ExecutionConfig.SerializableSerializer<?> asSerializerInstance() throws IllegalStateException {
		if (isSerializerInstance()) {
			return serializableSerializer;
		} else {
			throw new IllegalStateException();
		}
	}

	public static class SerializationProxy extends VersionedIOReadableWritable {

		private static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private DefaultSerializer defaultSerializer;

		public SerializationProxy(ClassLoader userCodeClassLoader) {}

		public SerializationProxy(DefaultSerializer defaultSerializer) {}

		public DefaultSerializer getDefaultSerializer() {
			return defaultSerializer;
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
