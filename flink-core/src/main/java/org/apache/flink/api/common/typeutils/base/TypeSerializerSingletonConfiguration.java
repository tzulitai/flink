package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.BasicTypeSerializerConfiguration;
import org.apache.flink.api.common.typeutils.Restorer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;

@Internal
@Restorer(TypeSerializerSingletonFactory.class)
public class TypeSerializerSingletonConfiguration<T> extends BasicTypeSerializerConfiguration<T> {

	public TypeSerializerSingletonConfiguration(Class<? extends TypeSerializerSingleton<T>> serializerSingletonClass) {
		super(serializerSingletonClass);
	}

	@Override
	public TypeSerializerFactory getFactory() {
		return null;
	}
}
