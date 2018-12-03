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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link ListSerializer}.
 */
public class ListSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<List<T>, ListSerializer<T>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedElementSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public ListSerializerSnapshot() {
		super();
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public ListSerializerSnapshot(ListSerializer<T> listSerializer) {
		super(listSerializer);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}
}
