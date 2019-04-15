#Storm Basics

## Basics

Apache Storm is realtime distributed stream processing framework.
Apache Storm Trident is a minibatch processing framework.
Apache Storm gurantees at-least-once where as Trident guarantees once-and-only-once processing.

Comparison:


|----------|----------------------|----------------|--------------
|Framework | Guranteed processing | Stream / Batch | Ordered
|----------|----------------------|----------------|--------------
|Storm     |  at least once       | stream         |  no
|Trident   |  once                | mini batch     |  no
|Spark     |  once                | mini batch     |  no
|Hadoop    |  once                | batch          |  -
|Flint     |  once                | stream         |  yes
|----------|----------------------|----------------|--------------


## Setting up a cluster

Apache Storm requires Java JDK and python 2.6+ on all nodes, and a running Zookeeper cluster. 

### Building apache storm docker image:

```
cd apache-storm_dockerfile
wget 'https://www-eu.apache.org/dist/storm/apache-storm-1.2.2/apache-storm-1.2.2.zip'
docker build -t apache-storm:1.2.2 .
```

### Building zookeeper docker image:

```
cd zookeeper_dockerfile
wget 'https://www-eu.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz'
docker build -t zookeeper:e.4.14 .
```
### Start a cluster

The essential parts of a Storm cluster are
- zookeeper: mamanges cluster state and coordinates different nodes
- nimbus: receives client's  topologies, distributes works to supervisor nodes.
- supervisor: manages worker processes, accepts works from nimbus nodes.

Optional parts are:
- ui: web interface
- drpc: receives and return DRPC calls form clients
  
```
docker stack deploy -c storm_cluster.yml storm_cluster
```

## Programming Concepts

### Spout, Bolt, Topology, Stream

A spont generates data. A bolt process data. A topology defines how spouts and bolts are connected.
Spouts can emit more than one stream. Bolts can receive and emits more than one stream.
Stream Id can be used to distinguish streams and tuples.

### Grouping

Stream flowing from one level to the next level can have different partitioning algorithm. Common ones are
- Shuffle grouping: Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
- Fields grouping: The stream is partitioned by the fields specified in the grouping. For example, if the stream is grouped by the "user-id" field, tuples with the same "user-id" will always go to the same task, but tuples with different "user-id"'s may go to different tasks.
- All grouping: The stream is replicated across all the bolt's tasks. Use this grouping with care.
- Global grouping
Others are
- Partial Key grouping: The stream is partitioned by the fields specified in the grouping, like the Fields grouping, but are load balanced between two downstream bolts, which provides better utilization of resources when the incoming data is skewed. This paper provides a good explanation of how it works and the advantages it provides.
- None grouping: This grouping specifies that you don't care how the stream is grouped. Currently, none groupings are equivalent to shuffle groupings. Eventually though, Storm will push down bolts with none groupings to execute in the same thread as the bolt or spout they subscribe from (when possible).
- Direct grouping: This is a special kind of grouping. A stream grouped this way means that the producer of the tuple decides which task of the consumer will receive this tuple. Direct groupings can only be declared on streams that have been declared as direct streams. Tuples emitted to a direct stream must be emitted using one of the [emitDirect](javadocs/org/apache/storm/task/OutputCollector.html#emitDirect(int, int, java.util.List) methods. A bolt can get the task ids of its consumers by either using the provided TopologyContext or by keeping track of the output of the emit method in OutputCollector (which returns the task ids that the tuple was sent to).
Local or shuffle grouping: If the target bolt has one or more tasks in the same worker process, tuples will be shuffled to just those in-process tasks. Otherwise, this acts like a normal shuffle grouping.

### Ack

Storm tracks all tuples. A tuples is ack-ed if it is explicitly ack-ed, or if all its derived tuples are ack-ed.
The generating spout will receive a `ack(tuple)` call if the tuple is acked, or `fail(tuple)` call if the tuple is timed-out.
It's the programmer's responsibility to provide proper error handling in `fail()` (such as resending the tuple) to guarantee all data are processed.

## Common patterns

- [Local cluster](StormLearning-java/src/main/java/lzhou/learning/storm/examples/LocalClusterExample.java)
- [Multiple streams](StormLearning-java/src/main/java/lzhou/learning/storm/examples/ControlStreamExample.java)
- [Windowed](StormLearning-java/src/main/java/lzhou/learning/storm/examples/MovingAverageExample.java)
- [Top K](StormLearning-java/src/main/java/lzhou/learning/storm/examples/TopkExample.java)
- [Stream Join](StormLearning-java/src/main/java/lzhou/learning/storm/examples/StreamJoinExample.java)
- [DRPC](StormLearning-java/src/main/java/lzhou/learning/storm/examples/DrpcExample.java)
