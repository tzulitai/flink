package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/**
 * Created by tzulitai on 20/06/2018.
 */
public class ListSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot<List<T>> {

	private final int VERSION = 1;

	public ListSerializerConfigSnapshot(TypeSerializer<T> elementSerializer) {
		super(elementSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<List<T>> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new ListSerializer<>((TypeSerializer<T>) restoredNestedSerializers[0]);
	}
}
