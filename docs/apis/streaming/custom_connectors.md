---
title: "Implementing Custom Streaming Connectors"

sub-nav-id: customconnectors
sub-nav-group: streaming
sub-nav-pos: 8
sub-nav-title: Custom Connectors
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

While Flink has an abundance of [supported streaming connectors](connectors/index.html), you might come across external
systems that are not yet supported by Flink. This page covers basic instructions on implementing new connectors.
The guide will reference implementations of currently existing connectors as examples at certain points.
For connectors that are not yet supported and frequently requested by the user community, it is highly encouraged to
contribute them for merging. For more details on contributing code to Flink, check out
the [Contributing Code](contributing_code.html) page.

* toc
{:toc}

## Overview

A connector typically consists of a source, sink, or both, depending on the target external system. For example,
systems such as [Apache Kafka](www.google.com). Sources and sinks are essentially user-defined functions that are split
into individual subtasks for parallelism, and are separately written by implementing corresponding base function interfaces.
The implemented base interfaces contain methods (ex., `run()` and `cancel()`) that will be called by the system throughput
the lifetime of each task.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

## Implementing Sources

Source functions will need to implement the `SourceFunction` interface. You can also choose to implement the `ParallelSourceFunction`
interface, which is simply an extending marker interface that tells the system to.
For more complicated sources where each parallel subtask needs to behave differently, you'll need to extend the `RichParallelSourceFunction`,
which provides additional access to the task's runtime context.

This guide attempts to instruct incrementally, starting with examples with implementing sources with fundamental functionality
to more complex, full-blown examples.

### Simple Example Source

The following example shows a simple example source implementing the `SourceFunction` interface. The source simply sends
from 1 to 1000 out to be processed by further operators downstream.

{% highlight java %}
public class SimpleExampleSource<T> implements SourceFunction<T> {
    private long count = 0L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<T> ctx) {
        while (isRunning && count < 1000) {
            ctx.collect(count);
            count++;
        }
    }

    @Override
    public void cancel() { isRunning = false; }
}
{% endhighlight %}

Sources implementing the `SourceFunction` are determined to be a non-parallel source, thus fixing the parallelism of the
source to 1, i.e. only one subtask will be executing the function. If the user attempts to explicitly set a parallelism value
above 1 for the source, the system will throw an exception complaining that the source is not parallel. For sources such as
the `TwitterSource` in the [Flink Twitter Connector](), this is a necessity since the Twitter Streaming API limits usage to
two concurrent connections. For more complex connectors such as the [Flink Kafka Connector]() which you'd implement a different
interface, explained later in this guide in [Parallel Sources]().

Like any other Flink function interface or class, you'd need to implement life cycle methods specific to the function. For
any source, you would need to implement at least the `run()` and `cancel()`. As the naming straightforwardly implies,
the `run()` method is called by the system when the source's subtasks have finished initialization and ready to be started.
Implementations can use the `SourceContext` instance to emit elements. Typically, it would contain a long-running while loop
to emit data.

{% highlight java %}
ctx.collectWithTimestamp(count, timestamp);
ctx.emitWatermark();
{% endhighlight %}

The `cancel()` method is called to interrupt the source, and should guarantee that once called, the long-running
loop in `run()` is escaped. A common pattern is to have a `volatile boolean isRunning` flag that is set to `false`, and checked
in the loop condition. It is good practice to make any flags altered by `cancel()` to be `volatile`, in order to guarantee
the visibility of the effects to any interruption handler.

### Parallel Sources

Below is an example of a parallel source that emits 1 to 1000, with each subtask responsible for emitting a subset of the range.

{% highlight java %}
public class SimpleExampleSource<T> extends RichParallelSourceFunction<T> {
    private long leftBound;
    private long rightBound;
    private long currentCount;

    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration configuration) {
        int totalNumOfConsumerSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int thisConsumerSubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        this.leftBound = calculateLeftBound(totalNumOfConsumerSubtasks, thisConsumerSubtaskIndex);
        this.rightBound = calculateRightBound(totalNumOfConsumerSubtasks, thisConsumerSubtaskIndex);
        this.currentCount = this.startingCount;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        while (isRunning && currentCount <= endingCount) {
            ctx.collect(currentCount);
            currentCount++;
        }
    }

    @Override
    public void cancel() { isRunning = false; }

    @Override
    public void close() { cancel(); }
}
{% endhighlight %}

Parallel sources need to implement the `ParallelSourceFunction` interface, which is a marker interface to let Flink allow
more than one subtask to be running the source. Also, parallel sources usually require each subtask to perform tasks differently.
For example, the [Flink Kafka Connector]() requires each subtask to connect to different Kafka partitions, depending on
the subtask's index. This can be achieved by extending the `RichParallelSourceFunction` abstract class instead, which provides
additional access to the runtime context of the subtask for information such as the total number of subtasks and the index
of the subtask.

The `RichParallelSourceFunction` also allows overriding two other source life cycle methods: `open()`, called before the parallel
subtask starts running, and `close()` to shutdown the source. Usually, the `open()` method should initialize the subtask's
state and decide what the subtask will be doing. The example demonstrates this by first getting the total number of subtasks
and the index of this subtask from the subtask's runtime context, and uses this information to decide the number range it
is responsible of emitting.

### Checkpointed Sources for Exactly-Once State Updates

The previous examples don't provide exactly-once guarantees; if a subtask fails mid-run and restarts, the `currentCount`
will start from the `leftBound` again. To achieve exactly once, the source will need to work with Flink's [Distributed Snapshots]()
checkpointing. The following extends the previous example to demonstrate this.

{% highlight java %}
public class SimpleExampleSource<T> extends RichParallelSourceFunction<T>, Checkpointed<Long> {
    // ...

    private Long currentCount;

    @Override
    public void open(Configuration configuration) {
        // ...

        // currentCount won't be null if it was restored from last checkpoint
        if (this.currentCount == null) {
            this.currentCount = this.leftBound;
        }
    }

    @Override
    public void run(SourceContext<T> ctx) {
        while (isRunning && currentCount <= endingCount) {
            synchronized(ctx.getCheckpointLock()) {
                ctx.collect(currentCount);
                currentCount++;
            }
        }
    }

    // ...

    @Override
    public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        return new Long(this.currentCount)
    }

    @Override
    public void restoreState(Long restoredState) throws Exception {
        this.currentCount = restoredState;
    }
}
{% endhighlight %}

------

By also implementing the `Checkpointed` interface, source functions can override the `snapshotState()` and `restoreState()`
methods just like any other checkpointed Flink user function. `snapshotState()` is called by Flink at checkpoint
barriers to extract the current state of the subtask, while `restoreState()` is called before `open()` to restore the
subtask's state from the last complete checkpoint.

Note that all updates to the subtask's state (i.e., `currentCount` in the example) must be synchronized with respect
to the checkpoint lock (accessed using `SourceContext.getCheckpointLock()`). Additionally, collecting the emitted record
and updating state must be done in a single atomic operation. When `snapshotState()` is called, the checkpoint lock is held
by Flink. This ensures that every snapshot state reflects only the records that have been processed and collected by the
source.

In the example, when `snapshotState()` is called, we simply return the `currentCount`. When `restoreState()` is called,
the `currentCount` is set to the restored value. Also, the `currentCount` is checked if it contains any restored value
already; if otherwise, the subtask is not restored from a checkpoint and we should start from the beginning.

Whether or not a source connector can be implemented with exactly-once guarantee also depends on the external system.
The external system will need to be "reliable" in that data will still be stored in the external system even after being
consumed by Flink. It'll also need to be "replayable" from a definable starting point. Again, take the
[Flink Kafka Connector]() for example. Apache Kafka retains data in each partition, and assigns offset ids to each record
which can be used to define where in each partition clients start reading from. To achieve exactly-once, the Flink Kafka
Connector maintains the internal state as the last processed offset of each consumed Kafka partition.

<span class="label label-danger">Exactly-Once</span> Note that the exactly-once guarantee of sources implies that all
records emitted from the source will affect internal state of all Flink operators in a topology exactly-once. It does not
mean a record is emitted exactly-once from the source. For more detail, please see [Distributed Snapshots]().

<span class="label label-info">Recommended</span> For sources that can continue to process data streams and mutate state
without mutating the returned state snapshot, it is recommended to implement the `CheckpointedAsynchronously` interface
instead. This marks the source to be "asynchronously checkpointed". Asynchronous checkpoints are desirable, because they
allow the data streams at the point of the checkpointed function/operator to continue running while the checkpoint is in
progress. Therefore, the example in this section can actually be safely changed to be asynchronously checkpointed because
it is always returning a copy of the `currentCount`.

## Implementing Sinks

### Simple Example Sink

Below is a simple example sink that prints each incoming record to the standard output, prefixed with the index of the
subtask that processed the record.

{% highlight java %}
public class SimplePrintSink<T> extends RichSinkFunction<T> {

    private PrintStream stream;
    private String prefix;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.stream = System.out;
        this.prefix = getRuntimeContext().getIndexOfThisSubtask() + "> ";
    }

    @Override
    public void invoke(IN record) {
        stream.println(prefix + record.toString());
    }

    @Override
    public void close() {
        this.stream = null;
    }
}
{% endhighlight %}

Sinks implement the `SinkFunction` interface and should override the `invoke()` method, which is executed by subtasks
on each incoming record. Like the example, you can also choose to use the extended `RichSinkFunction` instead to get
access to the runtime context, `open()` and `close()` life cycle methods if the parallel subtasks of the sink is to
perform differently.

The example demonstrates how each subtask

### Checkpointed Sinks for End-to-End Exactly-Once Delivery

In addition to exactly-once state updates, Flink can also provide *end-to-end exactly-once delivery* semantics for a
streaming topology if the data sink also participates with Flink's checkpointing mechanism. The definition of this guarantee
is that the final outputs of the topology to external systems will appear exactly-once. For non-idempotent writes and
updates to the external system, this is necessary for the external system to see the same results even when the streaming
topology fails.

The `SimplePrintSink` example in the previous section does not provide this guarantee, since the same record may be printed
more than once if the subtasks were restored back to a checkpoint that occurred before any dangling records which were not
part of a completed checkpoint yet.

Below is a demonstration of how to implement a sink that offers end-to-end exactly-once delivery semantics, assuming
that exactly-once state updates already hold (i.e., the source is also participating in Flink's checkpointing mechanism).

<span class="label label-danger">Cooperating with the external system</span> Note that since this example is relatively
simple compared to real-world connectors, the sink does not need to cooperate with the external system. Normally, for
more complex external systems acting as data sinks, end-to-end exactly-once delivery is only possible (generally in all
streaming processor systems) if the external system also participates with the sink's checkpointing. More detail on this
will be described later in this section, using the [Flink Hadoop FileSystem]() connector's exactly-once `RollingSink` as
an example.

{% highlight java %}
public class ExactlyOnceDeliveryPrintSink<T> extends RichSinkFunction<T>
    implements Checkpointed<HashMap<Long, List<String>>>, CheckpointListener {

    // ...

    private ArrayList<String> pendingNonCheckpointedRecords = new ArrayList<>();
    private final Object checkpointLock = new Object();
    private Map<Long, List<String>> pendingRecordsPerCheckpoint;

    @Override
    public void open(Configuration parameters) throws Exception {
        // ...
        if (this.pendingRecordsPerCheckpoint == null) {
            this.pendingRecordsPerCheckpoint = new HashMap<>();
        }
    }

    @Override
    public void invoke(T record) throws Exception {
        pendingNonCheckpointedRecords.add(prefix + record.toString());
    }

    @Override
    public HashMap<Long, List<String>> snapshotState(long checkpointId, long checkpointTimestamp)
        throws Exception {

        synchronized (checkpointLock) {
            this.pendingRecordsPerCheckpoint.put(checkpointId, pendingRecordsNonCheckpointedRecords);
        }
        this.pendingNonCheckpointedRecords = new ArrayList<>();
        return new HashMap<>(pendingRecordsPerCheckpoint);
    }

    @Override
    public void restoreState(HashMap<Long, List<String>> state) throws Exception {
        this.pendingRecordsPerCheckpoint = state;
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
        Set<Long> pastCheckpointIds = pendingRecordsPerCheckpoint.keySet();
        Set<Long> checkpointIdsToRemove = new HashSet<>();

        for (Long pastCheckpointId : pastCheckpointIds) {
            if (pastCheckpointId <= completedCheckpointId) {
                for (String record : this.pendingRecordsPerCheckpoint.get(pastCheckpointId)) {
                    System.out.println(record);
                }
                checkpointIdsToRemove.add(pastCheckpointId);
            }
        }

        for (Long checkpointIdToRemove : checkpointIdsToRemove) {
            this.pendingRecordsPerCheckpoint.remove(checkpointIdToRemove);
        }
    }
}
{% endhighlight %}

The problem with the original `SimplePrintSink` is that records were printed as soon as they arrive at the sink. If a
failure occurs and there were any printed records that had not been checkpointed, or the distributed snapshot of its checkpoint
was still in progress (thus will not be used to restore), these records will be replayed after the failure, resulting in
duplicate prints. In order to assure that the sink affects the external system (in this case, the standard output)
exactly-once even on failures, the sink should only print records after they are accounted in a completed checkpoint.

Additionally implementing the `CheckpointListener` interface gives access to the `notifyCheckpointComplete()` method, which
is called by Flink whenever the distributed snapshot of a checkpoint has fully completed. Combining this with the
`snapshotState()` method in the `Checkpointed` interface, sinks can maintain information about which records are part
of which checkpoint id as the sink function's user-defined state, and checkpoint this state in each snapshot. When the
`notifyCheckpointComplete()` is called, the sink can perform accordingly to the id of the completed checkpoint.

The `ExactlyOnceDeliveryPrintSink` demonstrates this. On each `invoke()` call, incoming records are appended to a list of
records that are awaiting to be checkpointed in the next snapshot. When `snapshotState()` is called, the pending records
are marked as checkpointed ,

## Other Connector Functionality

This section describes other useful functionality for connectors that can be added to source / sink functions.

 - *Monitoring source progress:*

 - *Allow connector users to provide a `DeserializationSchema` to convert consumed byte messages:*
