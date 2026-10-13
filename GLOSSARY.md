# SimpleKafka

The vocabulary of SimpleKafka, a from-scratch, educational Kafka-style distributed log: clusters of brokers that store topics as replicated, partitioned logs for producers and consumers.

## Cluster

**Cluster**:
The set of brokers that are registered together and serve the same topics.
_Avoid_: Group, farm

**Broker**:
A single server process in the cluster, identified by a broker id, that holds partitions.
_Avoid_: Node, server, instance

**Controller**:
The one broker, elected from the cluster, that is responsible for topic and partition placement.
_Avoid_: Master, coordinator, manager

**Metadata**:
A broker's description of the cluster: which brokers exist and, for each topic, which broker leads and which follow each partition.
_Avoid_: Cluster map, routing table

## Data

**Topic**:
A named stream of messages, split into one or more partitions.
_Avoid_: Queue, channel, stream

**Partition**:
One ordered, append-only log that holds a share of a topic's messages.
_Avoid_: Shard, slice

**Message**:
A single unit of data appended to a partition.
_Avoid_: Event, record, entry

**Offset**:
A message's position within its partition, starting at 0.
_Avoid_: Sequence number, id

**Segment**:
A consecutive, bounded slice of a partition's messages; a partition is a sequence of segments.
_Avoid_: Chunk, block

## Replication

**Replication factor**:
The number of brokers that hold a copy of each partition of a topic: the partition leader plus its followers.
_Avoid_: Copies, replica count

**Partition leader**:
The broker that is the authority for a partition: the one that holds its offset sequence and that clients produce to and fetch from.
_Avoid_: Master, primary, owner

**Follower**:
A broker that holds a copy of a partition and receives its messages from the partition leader.
_Avoid_: Slave, secondary

**Acknowledgement**:
The reply by which a partition leader tells a producer its message has been appended to the partition leader's own log (not necessarily to any follower).
_Avoid_: Receipt, commit

## Clients

**Producer**:
A client that appends messages to a topic.
_Avoid_: Publisher, writer, sender

**Consumer**:
A client that reads messages from a partition by offset.
_Avoid_: Subscriber, reader, listener
