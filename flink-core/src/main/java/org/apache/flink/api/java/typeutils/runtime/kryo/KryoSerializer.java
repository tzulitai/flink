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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.avro.generic.GenericData;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.objenesis.strategy.StdInstantiatorStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A type serializer that serializes its type using the Kryo serialization
 * framework (https://github.com/EsotericSoftware/kryo).
 * 
 * This serializer is intended as a fallback serializer for the cases that are
 * not covered by the basic types, tuples, and POJOs.
 *
 * @param <T> The type to be serialized.
 */
@Internal
public class KryoSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 3L;

	private static final Logger LOG = LoggerFactory.getLogger(KryoSerializer.class);

	// ------------------------------------------------------------------------

	private final Class<T> type;
	private LinkedHashMap<Class<?>, DefaultSerializer> defaults;
	private LinkedHashMap<Class<?>, RegisteredSerializer> registrations;

	// ------------------------------------------------------------------------

	private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultSerializers;
	private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses;
	private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypesWithSerializers;
	private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithSerializerClasses;
	private LinkedHashSet<Class<?>> registeredTypes;
	
	// ------------------------------------------------------------------------
	// The fields below are lazily initialized after duplication or deserialization.

	private transient Kryo kryo;
	private transient T copyInstance;
	
	private transient DataOutputView previousOut;
	private transient DataInputView previousIn;
	
	private transient Input input;
	private transient Output output;

	// ------------------------------------------------------------------------

	public KryoSerializer(Class<T> type, ExecutionConfig executionConfig){
		this.type = checkNotNull(type);

		checkNotNull(executionConfig);

		this.defaults = simulateDefaultSerializers(
				executionConfig.getDefaultKryoSerializers(),
				executionConfig.getDefaultKryoSerializerClasses());

		this.registrations = simulateRegistrations(
				type,
				executionConfig.getRegisteredKryoTypes(),
				executionConfig.getRegisteredTypesWithKryoSerializerClasses(),
				executionConfig.getRegisteredTypesWithKryoSerializers());
	}

	public KryoSerializer(
			Class<T> type,
			LinkedHashMap<Class<?>, DefaultSerializer> defaults,
			LinkedHashMap<Class<?>, RegisteredSerializer> registrations) {

		this.type = checkNotNull(type);
		this.defaults = checkNotNull(defaults);
		this.registrations = checkNotNull(registrations);
	}

	/**
	 * Copy-constructor that does not copy transient fields. They will be initialized once required.
	 */
	protected KryoSerializer(KryoSerializer<T> toCopy) {
		this.type = toCopy.type;
		this.defaults = toCopy.defaults;
		this.registrations = toCopy.registrations;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public T createInstance() {
		if(Modifier.isAbstract(type.getModifiers()) || Modifier.isInterface(type.getModifiers()) ) {
			return null;
		} else {
			checkKryoInitialized();
			try {
				return kryo.newInstance(type);
			} catch(Throwable e) {
				return null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T copy(T from) {
		if (from == null) {
			return null;
		}
		checkKryoInitialized();
		try {
			return kryo.copy(from);
		}
		catch(KryoException ke) {
			// kryo was unable to copy it, so we do it through serialization:
			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			Output output = new Output(baout);

			kryo.writeObject(output, from);

			output.close();

			ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
			Input input = new Input(bain);

			return (T)kryo.readObject(input, from.getClass());
		}
	}
	
	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if (target != previousOut) {
			DataOutputViewStream outputStream = new DataOutputViewStream(target);
			output = new Output(outputStream);
			previousOut = target;
		}

		// Sanity check: Make sure that the output is cleared/has been flushed by the last call
		// otherwise data might be written multiple times in case of a previous EOFException
		if (output.position() != 0) {
			throw new IllegalStateException("The Kryo Output still contains data from a previous " +
				"serialize call. It has to be flushed or cleared at the end of the serialize call.");
		}

		try {
			kryo.writeClassAndObject(output, record);
			output.flush();
		}
		catch (KryoException ke) {
			// make sure that the Kryo output buffer is cleared in case that we can recover from
			// the exception (e.g. EOFException which denotes buffer full)
			output.clear();

			Throwable cause = ke.getCause();
			if (cause instanceof EOFException) {
				throw (EOFException) cause;
			}
			else {
				throw ke;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(DataInputView source) throws IOException {
		checkKryoInitialized();
		if (source != previousIn) {
			DataInputViewStream inputStream = new DataInputViewStream(source);
			input = new NoFetchingInput(inputStream);
			previousIn = source;
		}

		try {
			return (T) kryo.readClassAndObject(input);
		} catch (KryoException ke) {
			Throwable cause = ke.getCause();

			if (cause instanceof EOFException) {
				throw (EOFException) cause;
			} else {
				throw ke;
			}
		}
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if(this.copyInstance == null){
			this.copyInstance = createInstance();
		}

		T tmp = deserialize(copyInstance, source);
		serialize(tmp, target);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return Objects.hash(
			type,
			registrations,
			defaultSerializerClasses);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof KryoSerializer) {
			KryoSerializer<?> other = (KryoSerializer<?>) obj;

			// we cannot include the Serializers here because they don't implement the equals method
			return other.canEqual(this) &&
				type == other.type &&
				registeredTypes.equals(other.registeredTypes) &&
				registeredTypesWithSerializerClasses.equals(other.registeredTypesWithSerializerClasses) &&
				defaultSerializerClasses.equals(other.defaultSerializerClasses);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof KryoSerializer;
	}

	@Override
	public KryoSerializerBuilder<T> getBuilder() {
		return new KryoSerializerBuilder<>(type, defaults, registrations);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the Chill Kryo Serializer which is implictly added to the classpath via flink-runtime.
	 * Falls back to the default Kryo serializer if it can't be found.
	 * @return The Kryo serializer instance.
	 */
	private Kryo getKryoInstance() {

		try {
			// check if ScalaKryoInstantiator is in class path (coming from Twitter's Chill library).
			// This will be true if Flink's Scala API is used.
			Class<?> chillInstantiatorClazz = Class.forName("com.twitter.chill.ScalaKryoInstantiator");
			Object chillInstantiator = chillInstantiatorClazz.newInstance();

			// obtain a Kryo instance through Twitter Chill
			Method m = chillInstantiatorClazz.getMethod("newKryo");

			return (Kryo) m.invoke(chillInstantiator);
		} catch (ClassNotFoundException | InstantiationException | NoSuchMethodException |
			IllegalAccessException | InvocationTargetException e) {

			LOG.warn("Falling back to default Kryo serializer because Chill serializer couldn't be found.", e);

			Kryo.DefaultInstantiatorStrategy initStrategy = new Kryo.DefaultInstantiatorStrategy();
			initStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

			Kryo kryo = new Kryo();
			kryo.setInstantiatorStrategy(initStrategy);

			return kryo;
		}
	}

	@SuppressWarnings("unchecked")
	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = getKryoInstance();

			// Enable reference tracking. 
			kryo.setReferences(true);

			// Add default serializers first, so that the type registrations without a serializer
			// are registered with a default serializer

			for (Map.Entry<Class<?>, DefaultSerializer> entry : defaults.entrySet()) {
				if (entry.getValue().isSerializerInstance()) {
					kryo.addDefaultSerializer(entry.getKey(), entry.getValue().asSerializerInstance().getSerializer());
				} else {
					kryo.addDefaultSerializer(entry.getKey(), entry.getValue().asSerializerClass());
				}
			}

			// registrations

			for (Map.Entry<Class<?>, RegisteredSerializer> registeredSerializerEntry : registrations.entrySet()) {
				RegisteredSerializer serializer = registeredSerializerEntry.getValue();

				if (serializer != null) {
					kryo.register(registeredSerializerEntry.getKey(), serializer.get(kryo));
				} else {
					kryo.register(registeredSerializerEntry.getKey());
				}
			}

			kryo.setRegistrationRequired(false);
			kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
		}
	}

	@VisibleForTesting
	public Kryo getKryo() {
		checkKryoInitialized();
		return this.kryo;
	}

	// ------------------------------------------------------------------------

	public static LinkedHashMap<Class<?>, DefaultSerializer> simulateDefaultSerializers(
			LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultSerializerInstances,
			LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses) {

		LinkedHashMap<Class<?>, DefaultSerializer> defaultSerializers = new LinkedHashMap<>();

		// Throwable and all subclasses should be serialized via java serialization
		// Note: the registered JavaSerializer is Flink's own implementation, and not Kryo's.
		//       This is due to a know issue with Kryo's JavaSerializer. See FLINK-6025 for details.
		defaultSerializers.put(Throwable.class, new DefaultSerializer(new ExecutionConfig.SerializableSerializer<>(new JavaSerializer())));

		for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultSerializerInstance : defaultSerializerInstances.entrySet()) {
			if (!defaultSerializers.containsKey(defaultSerializerInstance.getKey())) {
				defaultSerializers.put(defaultSerializerInstance.getKey(), new DefaultSerializer(defaultSerializerInstance.getValue()));
			}
		}

		for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClass : defaultSerializerClasses.entrySet()) {
			if (!defaultSerializers.containsKey(defaultSerializerClass.getKey())) {
				defaultSerializers.put(defaultSerializerClass.getKey(), new DefaultSerializer(defaultSerializerClass.getValue()));
			}
		}

		return defaultSerializers;
	}

	public static LinkedHashMap<Class<?>, RegisteredSerializer> simulateRegistrations(
			Class<?> serializedType,
			LinkedHashSet<Class<?>> registeredTypes,
			LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithSerializerClasses,
			LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypesWithSerializers) {

		LinkedHashMap<Class<?>, RegisteredSerializer> registrations = new LinkedHashMap<>();

		registrations.put(serializedType, null);

		for (Class<?> registeredType : checkNotNull(registeredTypes)) {
			registrations.put(registeredType, null);
		}

		for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> registeredTypeWithSerializerClass :
			checkNotNull(registeredTypesWithSerializerClasses).entrySet()) {
			registrations.put(
				registeredTypeWithSerializerClass.getKey(),
				new RegisteredSerializer(registeredTypeWithSerializerClass.getKey(), registeredTypeWithSerializerClass.getValue()));
		}

		for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypeWithSerializer :
			checkNotNull(registeredTypesWithSerializers).entrySet()) {
			registrations.put(
				registeredTypeWithSerializer.getKey(),
				new RegisteredSerializer(registeredTypeWithSerializer.getValue()));
		}

		// this is needed for Avro but can not be added on demand.
		registrations.put(
			GenericData.Array.class,
			new RegisteredSerializer(new ExecutionConfig.SerializableSerializer<>(new Serializers.SpecificInstanceCollectionSerializerForArrayList())));

		return registrations;
	}

	// ------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		this.defaults = simulateDefaultSerializers(defaultSerializers, defaultSerializerClasses);

		this.registrations = simulateRegistrations(
				type,
				registeredTypes,
				registeredTypesWithSerializerClasses,
				registeredTypesWithSerializers);
	}
}
