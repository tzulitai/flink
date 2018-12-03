package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Created by tzulitai on 29/11/2018.
 */
@PublicEvolving
public abstract class CompositeTypeSerializerSnapshot<T, S extends TypeSerializer<T>> implements TypeSerializerSnapshot<T> {

	private CompositeSerializerSnapshot compositeSerializerSnapshot;

	private final Class<S> correspondingSerializerClass;

	public CompositeTypeSerializerSnapshot(Class<S> correspondingSerializerClass) {
		this.correspondingSerializerClass = Preconditions.checkNotNull(correspondingSerializerClass);
	}

	public CompositeTypeSerializerSnapshot(S serializerInstance) {
		Preconditions.checkNotNull(serializerInstance);
		this.compositeSerializerSnapshot = new CompositeSerializerSnapshot(getNestedSerializers(serializerInstance));
		this.correspondingSerializerClass = serializerInstance.getClass();
	}

	@Override
	public final void writeSnapshot(DataOutputView out) throws IOException {
		writeOuterSnapshot(out);
		compositeSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		readOuterSnapshot(readVersion, in, userCodeClassLoader);
		this.compositeSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (newSerializer.getClass() != correspondingSerializerClass) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		S castedNewSerializer = correspondingSerializerClass.cast(newSerializer);

		// check that outer configuration is compatible; if not, short circuit result
		if (!isOuterSnapshotCompatible(castedNewSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// since outer configuration is compatible, the final compatibility result depends only on the nested serializers
		CompositeTypeSerializerSchemaCompatibilityBuilder finalCompatibilityBuilder =
			compositeSerializerSnapshot.getSchemaCompatibilityBuilder(
				getNestedSerializers(castedNewSerializer));
		return finalCompatibilityBuilder.build(this::createOuterSerializerWithNestedSerializers);
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		return createOuterSerializerWithNestedSerializers(compositeSerializerSnapshot.getRestoreSerializers());
	}

	// ------------------------------------------------------------------------------------------
	//  Outer serializer access methods
	// ------------------------------------------------------------------------------------------

	protected abstract TypeSerializer<?>[] getNestedSerializers(S outerSerializer);
	protected abstract S createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers);

	// ------------------------------------------------------------------------------------------
	// ------------------------------------------------------------------------------------------

	/**
	 *
	 * @param out
	 * @throws IOException
	 */
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {}

	/**
	 *
	 * @param readVersion
	 * @param in
	 * @param userCodeClassLoader
	 * @throws IOException
	 */
	protected void readOuterSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

	/**
	 *
	 * @param newSerializer
	 * @return
	 */
	protected boolean isOuterSnapshotCompatible(S newSerializer) {
		return true;
	}
}
