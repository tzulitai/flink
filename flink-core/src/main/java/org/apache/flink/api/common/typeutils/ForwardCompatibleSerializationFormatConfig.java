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

import org.apache.flink.annotation.PublicEvolving;

/**
 * This is a special {@link TypeSerializerConfigSnapshot} that serializers can return to serve
 * as a marker to indicate that new serializers for the data written by this serializer does not
 * need to be checked for compatibility.
 */
@PublicEvolving
public final class ForwardCompatibleSerializationFormatConfig extends ParameterlessTypeSerializerConfig {

	/** Singleton instance. */
	public static final ForwardCompatibleSerializationFormatConfig INSTANCE =
			new ForwardCompatibleSerializationFormatConfig();

	/**
	 * This special configuration type does not require the default
	 * empty nullary constructor because it will never actually be serialized.
	 */
	private ForwardCompatibleSerializationFormatConfig() {}
}
