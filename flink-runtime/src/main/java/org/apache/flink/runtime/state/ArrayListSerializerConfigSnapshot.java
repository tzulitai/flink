package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;

/**
 * Created by tzulitai on 20/06/2018.
 */
public class ArrayListSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot<ArrayList<T>> {

	private final int VERSION = 1;

	public ArrayListSerializerConfigSnapshot(TypeSerializer<T> elementSerializer) {
		super(elementSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<ArrayList<T>> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new ArrayListSerializer<>((TypeSerializer<T>) restoredNestedSerializers[0]);
	}
}
