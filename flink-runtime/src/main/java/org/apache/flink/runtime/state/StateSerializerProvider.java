package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.util.Preconditions;

/**
 * Created by tzulitai on 07/12/2018.
 */
public abstract class StateSerializerProvider<T> {

	public abstract TypeSerializer<T> getStateSerializer();

	public static <T> StateSerializerProvider<T> from(TypeSerializer<T> serializerInstance) {
		return new InstanceStateSerializerProvider<>(serializerInstance);
	}

	public static <T> StateSerializerProvider<T> from(TypeSerializerSnapshot<T> serializerSnapshot) {
		return new SnapshotStateSerializerProvider<>(serializerSnapshot);
	}

	private static class InstanceStateSerializerProvider<T> extends StateSerializerProvider<T> {

		private final TypeSerializer<T> serializerInstance;

		InstanceStateSerializerProvider(TypeSerializer<T> serializerInstance) {
			this.serializerInstance = Preconditions.checkNotNull(serializerInstance);
		}

		@Override
		public TypeSerializer<T> getStateSerializer() {
			return serializerInstance;
		}
	}

	private static class SnapshotStateSerializerProvider<T> extends StateSerializerProvider<T> {

		private final TypeSerializerSnapshot<T> serializerSnapshot;

		SnapshotStateSerializerProvider(TypeSerializerSnapshot<T> serializerSnapshot) {
			this.serializerSnapshot = Preconditions.checkNotNull(serializerSnapshot);
		}

		@Override
		public TypeSerializer<T> getStateSerializer() {
			return serializerSnapshot.restoreSerializer();
		}
	}
}
