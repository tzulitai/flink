package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Created by tzulitai on 04/10/2018.
 */
@Internal
public interface TypeSerializerSnapshot<T> {

	TypeSerializer<T> restoreSerializer();
	TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<?> newSerializer);
	void write(DataOutputView out) throws IOException;
	void read(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;
	int getVersion();

}
