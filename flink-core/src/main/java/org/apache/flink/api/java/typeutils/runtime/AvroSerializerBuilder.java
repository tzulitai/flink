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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializerBuilder;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class AvroSerializerBuilder<T> extends TypeSerializerBuilder {

	private static final int VERSION = 1;

	private Class<T> type;
	private Class<? extends T> typeToInstantiate;


	/** This empty nullary constructor is required for deserializing the builder. */
	public AvroSerializerBuilder() {}

	public AvroSerializerBuilder(Class<T> type, Class<? extends T> typeToInstantiate) {
		this.type = checkNotNull(type);
		this.typeToInstantiate = checkNotNull(typeToInstantiate);

		InstantiationUtil.checkForInstantiation(typeToInstantiate);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
