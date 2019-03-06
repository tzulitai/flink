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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.api.common.typeutils.ClassRelocator.RelocateClass;
import static org.junit.Assert.assertSame;

/**
 * A {@link TypeSerializerUpgradeTestBase} for the {@link PojoSerializer}.
 */
@RunWith(Parameterized.class)
public class PojoSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public PojoSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
		return Arrays.asList(
			new TestSpecification<>(
				"pojo-serializer-identical-schema",
				MigrationVersion.v1_7,
				IdenticalPojoSchemaSetup.class,
				IdenticalPojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-modified-schema",
				MigrationVersion.v1_7,
				ModifiedPojoSchemaSetup.class,
				ModifiedPojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-different-field-types",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class)
		);
	}

	// ----------------------------------------------------------------------------------------------
	//  Test specification setup & verifiers
	// ----------------------------------------------------------------------------------------------

	@SuppressWarnings("WeakerAccess")
	public static class StaticSchemaPojo {

		public enum Color {
			RED, BLUE, GREEN
		}

		public String name;
		public int age;
		public Color favoriteColor;
		public boolean married;

		public StaticSchemaPojo() {}

		public StaticSchemaPojo(String name, int age, Color favoriteColor, boolean married) {
			this.name = name;
			this.age = age;
			this.favoriteColor = favoriteColor;
			this.married = married;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (!(obj instanceof StaticSchemaPojo)) {
				return false;
			}

			StaticSchemaPojo other = (StaticSchemaPojo) obj;
			return Objects.equals(name, other.name)
				&& age == other.age
				&& favoriteColor == other.favoriteColor
				&& married == other.married;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age, favoriteColor, married);
		}
	}

	public static final class IdenticalPojoSchemaSetup implements PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojo("Gordon", 27, StaticSchemaPojo.Color.BLUE, false);
		}
	}

	public static final class IdenticalPojoSchemaVerifier implements UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo expectedTestData() {
			return new StaticSchemaPojo("Gordon", 27, StaticSchemaPojo.Color.BLUE, false);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	public static final class ModifiedPojoSchemaSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {

		@RelocateClass("TestPojoWithModifiedSchema")
		@SuppressWarnings("WeakerAccess")
		public static class PojoBeforeSchemaUpgrade {
			public int id;
			public String name;
			public int age;
			public double height;

			public PojoBeforeSchemaUpgrade() {}

			public PojoBeforeSchemaUpgrade(int id, String name, int age, double height) {
				this.id = id;
				this.name = name;
				this.age = age;
				this.height = height;
			}
		}

		@Override
		public TypeSerializer<PojoBeforeSchemaUpgrade> createPriorSerializer() {
			TypeSerializer<PojoBeforeSchemaUpgrade> serializer = TypeExtractor.createTypeInfo(PojoBeforeSchemaUpgrade.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoBeforeSchemaUpgrade createTestData() {
			return new PojoBeforeSchemaUpgrade(911108, "Gordon", 27, 172.8);
		}
	}

	public static final class ModifiedPojoSchemaVerifier implements UpgradeVerifier<ModifiedPojoSchemaVerifier.PojoAfterSchemaUpgrade> {

		@RelocateClass("TestPojoWithModifiedSchema")
		@SuppressWarnings("WeakerAccess")
		public static class PojoAfterSchemaUpgrade {

			public enum Color {
				RED, BLUE, GREEN
			}

			public String name;
			public int age;
			public Color favoriteColor;
			public boolean married;

			public PojoAfterSchemaUpgrade() {}

			public PojoAfterSchemaUpgrade(String name, int age, Color favoriteColor, boolean married) {
				this.name = name;
				this.age = age;
				this.favoriteColor = favoriteColor;
				this.married = married;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null) {
					return false;
				}

				if (!(obj instanceof PojoAfterSchemaUpgrade)) {
					return false;
				}

				PojoAfterSchemaUpgrade other = (PojoAfterSchemaUpgrade) obj;
				return Objects.equals(other.name, name)
					&& other.age == age
					&& other.favoriteColor == favoriteColor
					&& other.married == married;
			}

			@Override
			public int hashCode() {
				return Objects.hash(name, age, favoriteColor, married);
			}
		}

		@Override
		public TypeSerializer<PojoAfterSchemaUpgrade> createUpgradedSerializer() {
			TypeSerializer<PojoAfterSchemaUpgrade> serializer = TypeExtractor.createTypeInfo(PojoAfterSchemaUpgrade.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoAfterSchemaUpgrade expectedTestData() {
			return new PojoAfterSchemaUpgrade("Gordon", 27, null, false);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoAfterSchemaUpgrade>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAfterMigration();
		}
	}

	public static final class DifferentFieldTypePojoSchemaSetup implements PreUpgradeSetup<DifferentFieldTypePojoSchemaSetup.PojoWithIntField> {

		@RelocateClass("TestPojoWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class PojoWithIntField {

			public int fieldValue;

			public PojoWithIntField() {}

			public PojoWithIntField(int fieldValue) {
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<PojoWithIntField> createPriorSerializer() {
			TypeSerializer<PojoWithIntField> serializer = TypeExtractor.createTypeInfo(PojoWithIntField.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoWithIntField createTestData() {
			return new PojoWithIntField(911108);
		}
	}

	public static final class DifferentFieldTypePojoSchemaVerifier implements UpgradeVerifier<DifferentFieldTypePojoSchemaVerifier.PojoWithStringField> {

		@RelocateClass("TestPojoWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class PojoWithStringField {

			public String fieldValue;

			public PojoWithStringField() {}

			public PojoWithStringField(String fieldValue) {
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<PojoWithStringField> createUpgradedSerializer() {
			TypeSerializer<PojoWithStringField> serializer = TypeExtractor.createTypeInfo(PojoWithStringField.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoWithStringField expectedTestData() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoWithStringField>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isIncompatible();
		}
	}
}
