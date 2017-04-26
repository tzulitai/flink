package org.apache.flink.api.common.typeutils;

import org.apache.flink.util.InstantiationUtil;

public class BasicTypeSerializerFactory<T> implements TypeSerializerFactory<T, BasicTypeSerializerConfiguration<T>> {
	@Override
	public TypeSerializer<T> create(BasicTypeSerializerConfiguration<T> configuration) {
		return InstantiationUtil.instantiate(configuration.getSerializerClass());
	}
}
