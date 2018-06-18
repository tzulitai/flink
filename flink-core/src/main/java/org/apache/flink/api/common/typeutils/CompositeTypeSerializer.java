package org.apache.flink.api.common.typeutils;

/**
 * Created by tzulitai on 18/06/2018.
 */
public abstract class CompositeTypeSerializer<T> extends TypeSerializer<T> {

	@Override
	protected CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		if (isComparableSnapshot(configSnapshot)) {
			@SuppressWarnings("unchecked")
			CompositeTypeSerializerConfigSnapshot<T> snapshot = (CompositeTypeSerializerConfigSnapshot<T>) configSnapshot;

			int i = 0;
			for (TypeSerializer<?> nestedSerializer : getNestedSerializers()) {
				CompatibilityResult<?> compatibilityResult = CompatibilityUtil.resolveCompatibilityResult(
					snapshot.getNestedSerializersAndConfigs().get(i).f1,
					nestedSerializer);

				if (compatibilityResult.isRequiresMigration()) {
					return CompatibilityResult.requiresMigration();
				}

				i++;
			}

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	protected boolean isComparableSnapshot(TypeSerializerConfigSnapshot<?> configSnapshot) {
		return configSnapshot.getClass().equals(getSnapshotClass());
	}

	protected abstract Class<? extends CompositeSerializerSnapshot> getSnapshotClass();

	protected abstract TypeSerializer<?>[] getNestedSerializers();
}
