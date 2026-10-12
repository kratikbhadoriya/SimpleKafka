package com.simplekafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SimpleKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);
    private static final int DEFAULT_PARTITIONS = 3;
    private static final short  DEFAULT_REPLICATION_FACTOR = 2;

    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random;
    private final boolean createTopicIfNotExists;

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic, boolean createTopicIfNotExists) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.random = new Random();
        this.createTopicIfNotExists = createTopicIfNotExists;
    }

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this(bootstrapBroker, bootstrapPort, topic, true);
    }

    public void initialize() throws IOException {
        this.client.initialize();

        if((this.client.getTopicMetaData(this.topic) == null) && (createTopicIfNotExists)) {
            logger.info("Topic doesn't exist. Creating: {}", this.topic);
            boolean created = this.client.createTopic(this.topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
            if(!created){
                throw new IOException("Failed to create topic: " + this.topic);
            }
        }
    }

    public long send(String message) throws IOException {
        SimpleKafkaClient.TopicMetaData topicMetaData = this.client.getTopicMetaData(this.topic);
        if(topicMetaData == null){
            throw new IOException("Topic doesn't exist: " + this.topic);
        }

        int partitionCount = topicMetaData.partitions().size();
        int partition = this.random.nextInt(partitionCount);

        return send(message, partition);
    }

    public long send(String message, int partition) throws IOException {
        byte[] messageData = message.getBytes(StandardCharsets.UTF_8);
        return this.client.send(this.topic, partition, messageData);
    }

    public void close() throws IOException {
        // TODO: Perform cleanup operations here once the system gets more complex
        return;
    }

    public static void main(String[] args){
        if(args.length < 3){
            System.out.println("Usage: SimpleKafkaProducer <broker> <port> <topic>");
            System.exit(1);
        }

        String broker = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String topic = args[2];

        try{
            SimpleKafkaProducer producer = new SimpleKafkaProducer(broker, brokerPort, topic);
            producer.initialize();

            System.out.println("Producer initialized. Sending 10 messages....");

            for(int i = 0; i < 10; i++){
                String message = "Message " + i + "- " + System.currentTimeMillis();
                long offset = producer.send(message);
                System.out.println("Sent message to offset: " + offset);

                Thread.sleep(1000);
            }

            producer.close();
            System.out.println("Producer finished sending messages.");
        } catch (Exception e) {
            logger.error("Producer error", e);
        }
    }
}
