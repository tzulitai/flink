package org.apache.flink.api.common.typeutils;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Restorer {

	Class<? extends TypeSerializerFactory> value();

}
