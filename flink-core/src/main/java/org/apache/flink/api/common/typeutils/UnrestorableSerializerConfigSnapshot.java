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

import org.apache.flink.annotation.Internal;

/**
 * This interface is used as a tag to identify legacy {@link TypeSerializerConfigSnapshot}s that
 * cannot be used as a factory for the originating serializer of the config snapshot.
 *
 * <p>Since Flink 1.6.x, configuration snapshots serve also as a factory for the originating serializer
 * of the snapshot. This essentially means having a dedicated config snapshot class for every single serializer.
 * However, in older versions, since config snapshot classes were merely a container for serializer information and
 * not as a factory, some Flink serializers shared a common config snapshot class, which prohibited extending those
 * shared config snapshot classes to serve as a proper factory for specific serializers (i.e. it isn't possible
 * to know which exact serializer the restored config snapshot was extracted from).
 *
 * <p>By letting problematic config snapshot classes with this issue additionally implement the
 * {@code UnrestorableSerializerConfigSnapshot} interface, when Flink restores the config snapshot, it will wrap
 * it inside a {@link BackwardsCompatibleConfigSnapshot} to allow for backwards compatibility.
 */
@Internal
public interface UnrestorableSerializerConfigSnapshot {}
