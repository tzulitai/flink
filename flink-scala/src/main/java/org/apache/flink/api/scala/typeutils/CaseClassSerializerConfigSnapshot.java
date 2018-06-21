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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Created by tzulitai on 20/06/2018.
 */
public class CaseClassSerializerConfigSnapshot<T extends Product> extends CompositeTypeSerializerConfigSnapshot<T> {

	private final int VERSION = 1;

	private Class<T> typeClass;

	public CaseClassSerializerConfigSnapshot() {}

	public CaseClassSerializerConfigSnapshot(Class<T> typeClass, TypeSerializer<?>[] fieldSerializers) {
		super(checkNotNull(fieldSerializers));

		this.typeClass = checkNotNull(typeClass);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<T> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new CaseClassSerializer<T>(typeClass, restoredNestedSerializers) {

			private static final long serialVersionUID = -3126360534365716182L;

			@Override
			public T createInstance(Object[] fields) {
				try {
					return (T) typeClass.getConstructors()[0].newInstance(fields);
				} catch (Exception e) {
					throw new RuntimeException(
						"Error create instance of Scala case class " + typeClass.getName() +
							" with restored case class serializerr.", e);
				}
			}
		};
	}
}
