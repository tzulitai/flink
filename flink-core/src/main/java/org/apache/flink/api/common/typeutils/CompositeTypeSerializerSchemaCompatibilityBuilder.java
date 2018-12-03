package org.apache.flink.api.common.typeutils;

import com.sun.istack.internal.Nullable;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created by tzulitai on 29/11/2018.
 */
public class CompositeTypeSerializerSchemaCompatibilityBuilder {

	private TypeSerializerSchemaCompatibility<?> compatibilityResult;
	private TypeSerializer<?>[] reconfiguredNestedSerializers;

	public static CompositeTypeSerializerSchemaCompatibilityBuilder from(
			TypeSerializerSnapshot<?>[] nestedSerializerSnapshots,
			TypeSerializer<?>[] newNestedSerializers) {

		checkArgument(newNestedSerializers.length == nestedSerializerSnapshots.length,
			"Different number of new serializers and existing serializer snapshots.");

		TypeSerializer<?>[] reconfiguredNestedSerializers = new TypeSerializer[newNestedSerializers.length];

		// check nested serializers for compatibility
		boolean nestedSerializerRequiresMigration = false;
		boolean hasReconfiguredNestedSerializers = false;
		for (int i = 0; i < nestedSerializerSnapshots.length; i++) {
			TypeSerializerSchemaCompatibility<?> compatibility =
				resolveCompatibility(newNestedSerializers[i], nestedSerializerSnapshots[i]);

			// if any one of the new nested serializers is incompatible, we can just short circuit the result
			if (compatibility.isIncompatible()) {
				return new CompositeTypeSerializerSchemaCompatibilityBuilder(
					TypeSerializerSchemaCompatibility.incompatible(), null);
			}

			if (compatibility.isCompatibleAfterMigration()) {
				nestedSerializerRequiresMigration = true;
			} else if (compatibility.isCompatibleWithReconfiguredSerializer()) {
				hasReconfiguredNestedSerializers = true;
				reconfiguredNestedSerializers[i] = compatibility.getReconfiguredSerializer();
			} else if (compatibility.isCompatibleAsIs()) {
				reconfiguredNestedSerializers[i] = newNestedSerializers[i];
			} else {
				throw new IllegalStateException("Undefined compatibility type.");
			}
		}

		if (nestedSerializerRequiresMigration) {
			return new CompositeTypeSerializerSchemaCompatibilityBuilder(
				TypeSerializerSchemaCompatibility.compatibleAfterMigration(), null);
		}

		if (hasReconfiguredNestedSerializers) {
			return new CompositeTypeSerializerSchemaCompatibilityBuilder(null, reconfiguredNestedSerializers);
		}

		// ends up here if everything is compatible as is
		return new CompositeTypeSerializerSchemaCompatibilityBuilder(TypeSerializerSchemaCompatibility.compatibleAsIs(), null);
	}

	private CompositeTypeSerializerSchemaCompatibilityBuilder(
			@Nullable TypeSerializerSchemaCompatibility<?> compatibilityResult,
			@Nullable TypeSerializer<?>[] reconfiguredNestedSerializers) {

		Preconditions.checkArgument(!(reconfiguredNestedSerializers != null && compatibilityResult != null));
		this.compatibilityResult = compatibilityResult;
		this.reconfiguredNestedSerializers = reconfiguredNestedSerializers;
	}

	public <T> TypeSerializerSchemaCompatibility<T> build(SerializerFactory<T> serializerFactory) {
		if (compatibilityResult != null) {
			@SuppressWarnings("unchecked")
			TypeSerializerSchemaCompatibility<T> typedFinalCompatibilityResult = (TypeSerializerSchemaCompatibility<T>) compatibilityResult;

			return typedFinalCompatibilityResult;
		} else {
			return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(serializerFactory.create(reconfiguredNestedSerializers));
		}
	}

	public interface SerializerFactory<T> {
		TypeSerializer<T> create(TypeSerializer<?>[] nestedSerializers);
	}

	@SuppressWarnings("unchecked")
	private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
		TypeSerializer<?> serializer,
		TypeSerializerSnapshot<?> snapshot) {

		TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
		TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

		return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
	}

}
