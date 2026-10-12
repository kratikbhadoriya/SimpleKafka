package com.simplekafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private static final int MAX_POLL_BYTES = 1024 * 1024;
    private static final int POLL_INTERVAL_MS = 1000;

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private final AtomicBoolean isRunning;
    private long currentOffset;
    private Thread consumerThread;


    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset){
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startOffset;
        this.isRunning = new AtomicBoolean(false);
        this.consumerThread = null;
    }

    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition){
        this(bootstrapBroker, bootstrapPort, topic, partition, 0);
    }

    public void initialize() throws IOException {
        this.client.initialize();

        if(this.client.getTopicMetaData(this.topic) == null){
            throw new IOException("Topic doesn't exist: " + this.topic);
        }
    }

    public void close(){
        stopConsuming();
    }
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = this.client.fetch(this.topic, this.partition, this.currentOffset, MAX_POLL_BYTES);
        if(!messages.isEmpty()){
            this.currentOffset += messages.size();
        }
        return messages;
    }

    public void startConsuming(MessageHandler messageHandler) {
        if(this.isRunning.compareAndSet(false, true)){
            this.consumerThread = new Thread(() -> {
                try{
                    while(this.isRunning.get()){
                        List<byte[]> messages = poll();
                        for(byte[] message : messages){
                            messageHandler.handle(message, this.currentOffset - messages.size() + messages.indexOf(message));
                        }

                        if(messages.isEmpty()){
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if(this.isRunning.get()){
                        logger.error("Error in consumer loop", e);
                    }
                    this.isRunning.set(false);
                }
            });
            this.consumerThread.setDaemon(true);
            this.consumerThread.start();
        }
    }

    public void stopConsuming() {
        if(this.isRunning.compareAndSet(true, false)){
            if(this.consumerThread != null){
                try{
                    this.consumerThread.interrupt();
                    this.consumerThread.join(1000);
                } catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Stopped consuming from topic: {}, partition: {}", this.topic, this.partition);
        }
    }

    public void seek(long offset){
        this.currentOffset = offset;
    }

    public long getCurrentOffset(){
        return this.currentOffset;
    }

    public interface MessageHandler{
        void handle(byte[] message, long currentOffset);
    }

    public static void main(String[] args){
        if(args.length < 4){
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition>");
            System.exit(1);
        }

        String broker = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);

        try{
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, brokerPort, topic, partition);
            consumer.initialize();

            System.out.println("Consumer initialize. Starting consumption....");

            // Consume messages and print them
            consumer.startConsuming(((message, offset) ->{
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("Message received at offset " + offset + ": " +  messageStr);
            } ));

            System.out.println("Consumer started. Press enter to stop...");
            System.in.read();

            consumer.close();
            System.out.println("Consumer stopped.");
        } catch (Exception e) {
            logger.error("Consumer error", e);
        }
    }
}
