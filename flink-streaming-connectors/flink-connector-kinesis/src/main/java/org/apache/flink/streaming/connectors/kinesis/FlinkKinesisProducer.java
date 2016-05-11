/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kinesis;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisRecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisRecordResult;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The FlinkKinesisProducer allows to produce from a Flink DataStream into Kinesis.
 *
 * @param <OUT> Data type to produce into Kinesis Streams
 */
public class FlinkKinesisProducer<OUT> extends RichSinkFunction<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisProducer.class);

	/* AWS region of the stream */
	private final String region;

	/* Access and secret key of the user */
	private final String accessKey;
	private final String secretKey;

	/* Flag controlling the error behavior of the producer */
	private boolean failOnError = false;

	/* Name of the default stream to produce to. Can be overwritten by the serialization schema */
	private String defaultStream;

	/* Default partition id. Can be overwritten by the serialization schema */
	private String defaultPartition;

	/* Schema for turning the OUT type into a byte array. */
	private final KinesisSerializationSchema<OUT> schema;

	/* Optional custom partitioner */
	private KinesisPartitioner<OUT> customPartitioner = null;


	// --------------------------- Runtime fields ---------------------------


	/* Our Kinesis instance for each parallel Flink sink */
	private transient KinesisRecordPublisher recordPublisher;

	/* Callback handling failures */
	private transient FutureCallback<KinesisRecordResult> callback;

	/* Field for async exception */
	private transient volatile Throwable thrownException;


	// --------------------------- Initialization and configuration  ---------------------------


	/**
	 * Create a new FlinkKinesisProducer.
	 * This is a constructor supporting Flink's {@see SerializationSchema}.
	 *
	 * @param region AWS region of the stream
	 * @param accessKey Access key of a user with permission to access the stream (ideally also with access to Cloud Watch)
	 * @param secretKey Secret key of the user
	 * @param schema Serialization schema for the data type
	 */
	public FlinkKinesisProducer(String region, String accessKey, String secretKey, final SerializationSchema<OUT> schema) {
		// create a simple wrapper for the serialization schema
		this(region, accessKey, secretKey, new KinesisSerializationSchema<OUT>() {
			@Override
			public ByteBuffer serialize(OUT element) {
				// wrap into ByteBuffer
				return ByteBuffer.wrap(schema.serialize(element));
			}
			// use default stream and hash key
			@Override
			public String getTargetStream(OUT element) {
				return null;
			}
		});
	}

	public FlinkKinesisProducer(String region, String accessKey, String secretKey, KinesisSerializationSchema<OUT> schema) {
		this.region = Objects.requireNonNull(region);
		this.accessKey = Objects.requireNonNull(accessKey);
		this.secretKey = Objects.requireNonNull(secretKey);
		ClosureCleaner.ensureSerializable(Objects.requireNonNull(schema));
		this.schema = schema;
	}

	/**
	 * If set to true, the producer will immediately fail with an exception on any error.
	 * Otherwise, the errors are logged and the producer goes on.
	 *
	 * @param failOnError Error behavior flag
	 */
	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	/**
	 * Set a default stream name.
	 * @param defaultStream Name of the default Kinesis stream
	 */
	public void setDefaultStream(String defaultStream) {
		this.defaultStream = defaultStream;
	}

	/**
	 * Set default partition id
	 * @param defaultPartition Name of the default partition
	 */
	public void setDefaultPartition(String defaultPartition) {
		this.defaultPartition = defaultPartition;
	}

	public void setCustomPartitioner(KinesisPartitioner<OUT> partitioner) {
		Objects.requireNonNull(partitioner);
		ClosureCleaner.ensureSerializable(partitioner);
		this.customPartitioner = partitioner;
	}


	// --------------------------- Lifecycle methods ---------------------------


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		recordPublisher = new KinesisRecordPublisher(region, accessKey, secretKey, getRuntimeContext().getTaskName());
		callback = new FutureCallback<KinesisRecordResult>() {
			@Override
			public void onSuccess(KinesisRecordResult result) {
				// if successful, simply ignore and continue
			}

			// if failed, the record publisher will throw a KinesisRecordFailedException
			@Override
			public void onFailure(Throwable t) {
				if (failOnError) {
					thrownException = t;
				} else {
					LOG.warn("An exception occurred while processing a record", t);
				}
			}
		};

		if (this.customPartitioner != null) {
			this.customPartitioner.initialize(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
		}

		LOG.info("Started Kinesis Record Publisher instance for region '{}'", this.region);
	}

	@Override
	public void invoke(OUT value) throws Exception {

		if (this.recordPublisher == null) {
			throw new RuntimeException("Kinesis Record Publisher has been closed");
		}

		if (thrownException != null) {
			if (failOnError) {
				throw new RuntimeException("An exception was thrown while processing a record: " + thrownException.getMessage(), thrownException);
			} else {
				LOG.warn("An exception was thrown while processing a record: {}", thrownException.getMessage(), thrownException);
				thrownException = null; // reset
			}
		}

		String stream = defaultStream;
		String partition = defaultPartition;

		ByteBuffer serialized = schema.serialize(value);

		// maybe set custom stream
		String customStream = schema.getTargetStream(value);
		if (customStream != null) {
			stream = customStream;
		}

		String explicitHashkey = null;
		// maybe set custom partition
		if (customPartitioner != null) {
			partition = customPartitioner.getPartitionId(value);
			explicitHashkey = customPartitioner.getExplicitHashKey(value);
		}

		if (stream == null) {
			if (failOnError) {
				throw new RuntimeException("No target stream set");
			} else {
				LOG.warn("No target stream set. Skipping record");
				return;
			}
		}

		ListenableFuture<KinesisRecordResult> cb = recordPublisher.addRecordToPublish(stream, serialized, partition, explicitHashkey);
		Futures.addCallback(cb, callback);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing producer");
		super.close();
		KinesisRecordPublisher tmpRecordPublisher = this.recordPublisher;

		// setting to null to forbid any new records to be added for publishing
		this.recordPublisher = null;

		if (tmpRecordPublisher != null) {

			LOG.info("Shutting down record publisher instance.");
			tmpRecordPublisher.shutdown();

			LOG.info("Awaiting outstanding {} records to be published.", tmpRecordPublisher.getOutstandingRecordsCount());
			// wait for all outstanding records to be published
			while (tmpRecordPublisher.getOutstandingRecordsCount() > 0) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					LOG.warn("Interrupted while waiting for remaining outstanding records to be published.");
					// stop and destroy record publisher immediately
					break;
				}
			}

			LOG.info("All outstanding records published. Destroying record publisher instance.");
			tmpRecordPublisher.destroy();
		}
	}

}
