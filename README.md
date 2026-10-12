# SimpleKafka

A from-scratch, educational Kafka-style distributed log in Java.

## Quick Start

### Prerequisites

- JDK 25 and Maven (needed for the plain `java -jar` path; Docker builds the jar itself)
- Docker with Compose (optional; only for the Docker Compose path)

### Option 1: Docker Compose (ZooKeeper + 3 brokers)

```bash
docker compose up --build
```

The first run builds the shaded jar inside Docker and pulls the ZooKeeper image.
Wait until the logs show `Elected controller is ...` and each broker prints
`SimpleKafka broker started`. The brokers start one after another (a few
seconds apart); that is intentional, because brokers started at the same instant
can end up with an incomplete view of the cluster.

Brokers 1, 2 and 3 listen on `localhost:9092`, `9093` and `9094`, but only
inside the cluster's network namespace; no ports are published to the host.
Run the clients inside a broker container (in a second terminal):

```bash
# Produce 10 messages to topic "demo" (the producer creates the topic
# with 3 partitions and replication factor 2 if it does not exist)
docker compose exec -T broker1 java -cp /app/simplekafka.jar \
  com.simplekafka.client.SimpleKafkaProducer localhost 9092 demo

# Consume partition 0 of "demo" (press Enter to stop; use partition 0, 1 or 2)
docker compose exec broker1 java -cp /app/simplekafka.jar \
  com.simplekafka.client.SimpleKafkaConsumer localhost 9092 demo 0
```

Messages are sent to a random partition, so consume partitions 0, 1 and 2 to
see all 10 messages. Expected consumer output:

```
Message received at offset 0: Message 1- 1791829988307
Message received at offset 1: Message 2- 1791829989569
...
```

Stop and clean up with `docker compose down -v`.

### Option 2: plain `java -jar`

The broker looks for ZooKeeper on `localhost`. Start one (any ZooKeeper 3.x
works; here via Docker, mapped to host port 2182 so it does not clash with a
ZooKeeper already running on 2181):

```bash
docker run -d --rm --name sk-zk -p 2182:2181 zookeeper:3.9
```

Build the jar:

```bash
mvn package -DskipTests
```

Start three brokers, each in its own terminal, a few seconds apart. Arguments
are `<brokerId> <brokerHost> <brokerPort> [zooKeeperPort]` (ZooKeeper port
defaults to 2181):

```bash
java -jar target/SimpleKafka-1.0-SNAPSHOT.jar 1 localhost 9092 2182
java -jar target/SimpleKafka-1.0-SNAPSHOT.jar 2 localhost 9093 2182
java -jar target/SimpleKafka-1.0-SNAPSHOT.jar 3 localhost 9094 2182
```

The first broker logs `Elected controller is 1`. Brokers write their logs under
`./data` in the directory they are started from.

Produce and consume (in two more terminals):

```bash
java -cp target/SimpleKafka-1.0-SNAPSHOT.jar \
  com.simplekafka.client.SimpleKafkaProducer localhost 9092 demo

java -cp target/SimpleKafka-1.0-SNAPSHOT.jar \
  com.simplekafka.client.SimpleKafkaConsumer localhost 9092 demo 0
```

Producer arguments: `<broker> <port> <topic>`. Consumer arguments:
`<broker> <port> <topic> <partition>`.
