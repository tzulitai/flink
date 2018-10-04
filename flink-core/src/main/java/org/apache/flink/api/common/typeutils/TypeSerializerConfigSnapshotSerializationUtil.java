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

import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Utility methods for serialization of {@link TypeSerializerConfigSnapshot}.
 */
public class TypeSerializerConfigSnapshotSerializationUtil {

	/**
	 * Writes a {@link TypeSerializerConfigSnapshot} to the provided data output view.
	 *
	 * <p>It is written with a format that can be later read again using
	 * {@link #readSerializerConfigSnapshot(DataInputView, ClassLoader, TypeSerializer)}.
	 *
	 * @param out the data output view
	 * @param serializerConfigSnapshot the serializer configuration snapshot to write
	 *
	 * @throws IOException
	 */
	public static <T> void writeSerializerConfigSnapshot(
		DataOutputView out,
		PersistedTypeSerializer<T> serializerConfigSnapshot,
		TypeSerializer<T> serializer) throws IOException {

		new TypeSerializerConfigSnapshotSerializationProxy<>(serializerConfigSnapshot, serializer).write(out);
	}

	/**
	 * Reads from a data input view a {@link TypeSerializerConfigSnapshot} that was previously
	 * written using {@link TypeSerializerConfigSnapshotSerializationUtil#writeSerializerConfigSnapshot(DataOutputView, TypeSerializerConfigSnapshot, TypeSerializer)}.
	 *
	 * @param in the data input view
	 * @param userCodeClassLoader the user code class loader to use
	 *
	 * @return the read serializer configuration snapshot
	 *
	 * @throws IOException
	 */
	public static <T> PersistedTypeSerializer<T> readSerializerConfigSnapshot(
			DataInputView in,
			ClassLoader userCodeClassLoader,
			@Nullable TypeSerializer<T> existingPriorSerializer) throws IOException {

		final TypeSerializerConfigSnapshotSerializationProxy<T> proxy =
			new TypeSerializerConfigSnapshotSerializationProxy<>(userCodeClassLoader, existingPriorSerializer);
		proxy.read(in);

		return proxy.getSerializerConfigSnapshot();
	}

	/**
	 * Utility serialization proxy for a {@link TypeSerializerConfigSnapshot}.
	 */
	static final class TypeSerializerConfigSnapshotSerializationProxy<T> extends VersionedIOReadableWritable {

		private static final int VERSION = 2;

		private ClassLoader userCodeClassLoader;
		private PersistedTypeSerializer<T> serializerConfigSnapshot;
		private TypeSerializer<T> serializer;

		TypeSerializerConfigSnapshotSerializationProxy(
			ClassLoader userCodeClassLoader,
			@Nullable TypeSerializer<T> existingPriorSerializer) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
			this.serializer = existingPriorSerializer;
		}

		TypeSerializerConfigSnapshotSerializationProxy(
			PersistedTypeSerializer<T> serializerConfigSnapshot,
			TypeSerializer<T> serializer) {
			this.serializerConfigSnapshot = Preconditions.checkNotNull(serializerConfigSnapshot);
			this.serializer = Preconditions.checkNotNull(serializer);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			// config snapshot class, so that we can re-instantiate the
			// correct type of config snapshot instance when deserializing
			out.writeUTF(serializerConfigSnapshot.getClass().getName());

			if (serializerConfigSnapshot instanceof TypeSerializerSnapshot) {
				out.writeBoolean(false); //flag indicating that no serializer is present

				TypeSerializerSnapshot<T> serializerSnapshot = (TypeSerializerSnapshot<T>) serializerConfigSnapshot;

				// write version
				out.writeInt(serializerSnapshot.getVersion());
				serializerSnapshot.write(out);
			} else if (serializerConfigSnapshot instanceof TypeSerializerConfigSnapshot) {
				out.writeBoolean(true); //flag indicating that no serializer is present

				@SuppressWarnings("unchecked")
				TypeSerializerConfigSnapshot<T> legacySerializerSnapshot = (TypeSerializerConfigSnapshot<T>) serializerConfigSnapshot;

				TypeSerializerSerializationUtil.writeSerializer(out, serializer);
				legacySerializerSnapshot.write(out);
			} else {
				throw new IllegalStateException();
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			String serializerConfigClassname = in.readUTF();
			Class<? extends PersistedTypeSerializer> serializerConfigSnapshotClass;
			try {
				serializerConfigSnapshotClass = (Class<? extends PersistedTypeSerializer>)
					Class.forName(serializerConfigClassname, true, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(
					"Could not find requested TypeSerializerConfigSnapshot class "
						+ serializerConfigClassname +  " in classpath.", e);
			}

			serializerConfigSnapshot = InstantiationUtil.instantiate(serializerConfigSnapshotClass);

			if (getReadVersion() >= 2) {
				boolean containsPriorSerializer = in.readBoolean();

				TypeSerializer<T> priorSerializer = (containsPriorSerializer)
					? TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true)
					: null;

				if (serializerConfigSnapshot instanceof TypeSerializerSnapshot) {
					TypeSerializerSnapshot<T> restoredSerializerSnapshot = (TypeSerializerSnapshot<T>) serializerConfigSnapshot;


				} else if (serializerConfigSnapshot instanceof TypeSerializerConfigSnapshot) {
					if (priorSerializer != null) {

					} else {

					}
				} else {
					throw new IllegalStateException();
				}
			} else {
				if (serializerConfigSnapshot instanceof TypeSerializerConfigSnapshot) {
					TypeSerializerConfigSnapshot<T> legacySerializerSnapshot = (TypeSerializerConfigSnapshot<T>) serializerConfigSnapshot;
					legacySerializerSnapshot.setPriorSerializer(this.serializer);
					legacySerializerSnapshot.read(in);
				} else if (serializerConfigSnapshot instanceof TypeSerializerSnapshot) {
					TypeSerializerSnapshot<T> serializerSnapshot = (TypeSerializerSnapshot<T>) serializerConfigSnapshot;

					int readVersion = in.readInt();
					serializerSnapshot.read(readVersion, in, userCodeClassLoader);
				}
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public int[] getCompatibleVersions() {
			return new int[]{VERSION, 1};
		}

		TypeSerializerConfigSnapshot<T> getSerializerConfigSnapshot() {
			return serializerConfigSnapshot;
		}
	}
}
