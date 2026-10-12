package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaClient {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaClient.class);
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetaData> topicMetaData;
    private final Map<Integer, BrokerInfo> brokers;

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetaData = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
    }

    public void initialize() throws IOException {
        refreshMetaData();
    }

    /**
     * Refresh metadata about brokers and topic
     * @throws IOException Socket handling errors, No data read in buffer errors
     */
    public void refreshMetaData() throws IOException {
        SocketChannel brokerChannel = SocketChannel.open();
        brokerChannel.connect(new InetSocketAddress(this.bootstrapBroker, this.bootstrapPort));
        ByteBuffer requestBuffer = Protocol.encodeMetadataRequest();
        brokerChannel.write(requestBuffer);

        ByteBuffer responseBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        int bytesRead = brokerChannel.read(responseBuffer);
        if(bytesRead <= 0){
            throw new IOException("No data received from broker");
        }
        responseBuffer.flip();
        Protocol.MetadataResult metadataResult = Protocol.decodeMetadataResponse(responseBuffer);

        if(!metadataResult.isSuccess()){
            throw new IOException("Failed to fetch metadata: " + metadataResult.error());
        }

        this.brokers.clear();
        for(BrokerInfo brokerInfo : metadataResult.brokers()){
            this.brokers.put(brokerInfo.getId(), new BrokerInfo(brokerInfo.getId(), brokerInfo.getHost(), brokerInfo.getPort()));
        }

        this.topicMetaData.clear();
        for(Protocol.TopicMetaData topic : metadataResult.topics()){

            List<PartitionInfo> partitions = new ArrayList<>();
            for(Protocol.PartitionMetaData partition : topic.partitions()){
                partitions.add(new PartitionInfo(partition.id(), partition.leadBrokerId(), partition.replicasBrokerIds()));
            }

            this.topicMetaData.put(topic.topic(), new TopicMetaData(topic.topic(), partitions));
        }

        logger.info("Metadata refreshed successfully: {} brokers, {} topics", this.brokers.size(), this.topicMetaData.size());
    }

    /**
     * Produce a message to a topic-partition
     * @param topic Target topic
     * @param partitionId Target partition ID
     * @param message Message to be produced
     * @return Message offset in the target partition
     * @throws IOException Topic not found
     * OR
     * Partition not found
     * OR
     * Leader not found
     * OR
     * No message received from the leader
     * OR
     * Failed to produce message
     */
    public long send(String topic, int partitionId, byte[] message) throws IOException {
        if(!this.topicMetaData.containsKey(topic)) {
            refreshMetaData();
            if(!this.topicMetaData.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        TopicMetaData metadata = this.topicMetaData.get(topic);
        PartitionInfo partition = null;

        for(PartitionInfo partitionInfo : metadata.partitions()){
            if(partitionInfo.id() == partitionId){
                partition = partitionInfo;
                break;
            }
        }

        if(partition == null){
            throw new IOException("Partition not found: " + partitionId);
        }

        int leaderId = partition.leader();
        BrokerInfo leader = this.brokers.get(leaderId);

        if(leader == null){
            refreshMetaData();
            leader = this.brokers.get(leaderId);
            if(leader == null){
                throw new IOException("Leader not found: " + leaderId);
            }
        }

        try(SocketChannel leaderChannel = SocketChannel.open()){
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            ByteBuffer requestBuffer = Protocol.encodeProduceRequest(topic, partitionId, message);
            leaderChannel.write(requestBuffer);

            ByteBuffer responseBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = leaderChannel.read(responseBuffer);
            if(bytesRead <= 0){
                throw new IOException("No data received from broker");
            }
            responseBuffer.flip();

            Protocol.ProduceResult produceResult = Protocol.decodeProduceResponse(responseBuffer);
            if(!produceResult.isSuccess()){
                throw new IOException("Failed to produce message: " + produceResult.error());
            }
            return produceResult.offset();
        }
    }

    /**
     * Consume messages from a topic-partition
     * @param topic Target topic
     * @param partitionId Target partition ID
     * @param offset Initial target offset
     * @param maxBytes Maximum bytes to be fetched
     * @return List of messages starting from the intial target offset
     * @throws IOException Topic not found
     * OR
     * Partition not found
     * OR
     * Leader not found
     * OR
     * No message received from the leader
     * OR
     * Failed to fetch messages
     */
    public List<byte[]> fetch(String topic, int partitionId, long offset, int maxBytes) throws IOException {
        if(!this.topicMetaData.containsKey(topic)) {
            refreshMetaData();
            if(!this.topicMetaData.containsKey(topic)) {
                throw new IOException("Topic doesn't exist: " + topic);
            }
        }

        // Find leader for partition
        TopicMetaData metadata = this.topicMetaData.get(topic);
        PartitionInfo partition = null;
        for(PartitionInfo partitionInfo : metadata.partitions()){
            if(partitionInfo.id() == partitionId){
                partition = partitionInfo;
                break;
            }
        }

        if(partition == null){
            throw new IOException("Partition not found: " + partitionId);
        }

        int leaderId = partition.leader();
        BrokerInfo leader = this.brokers.get(leaderId);
        if(leader == null){
            refreshMetaData();
            leader = this.brokers.get(leaderId);
            if(leader == null){
                throw new IOException("Leader not found: " + leaderId);
            }
        }

        // Fetch from leader
        try(SocketChannel leaderChannel = SocketChannel.open()){
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Send fetch request
            ByteBuffer requestBuffer = Protocol.encodeFetchRequest(topic, partitionId, offset, maxBytes);
            leaderChannel.write(requestBuffer);

            // Read response
            ByteBuffer responseBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = leaderChannel.read(responseBuffer);
            if(bytesRead <= 0){
                throw new IOException("No data received from broker");
            }
            responseBuffer.flip();
            Protocol.FetchResult fetchResult = Protocol.decodeFetchResponse(responseBuffer);

            if(!fetchResult.isSuccess()){
                throw new IOException("Failed to fetch messages: " + fetchResult.error());
            }

            List<byte[]> messages = new ArrayList<>();
            for(byte[] message : fetchResult.messages()){
                messages.add(message);
            }
            return messages;
        }
    }

    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        if(this.brokers.isEmpty()){
            refreshMetaData();
            if(this.brokers.isEmpty()){
                throw new IOException("No brokers available");
            }
        }

        BrokerInfo broker = this.brokers.values().iterator().next();

        try(SocketChannel brokerChannel = SocketChannel.open()){
            brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
            ByteBuffer requestBuffer = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            brokerChannel.write(requestBuffer);

            ByteBuffer responseBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = brokerChannel.read(responseBuffer);
            if(bytesRead <= 0){
                throw new IOException("No data received from broker");
            }
            responseBuffer.flip();
            byte responseType = responseBuffer.get();
            if(responseType != Protocol.CREATE_TOPIC_RESPONSE){
                if(responseType == Protocol.ERROR_RESPONSE){
                    short errorLength = responseBuffer.getShort();
                    byte[] errorBytes = new byte[errorLength];
                    responseBuffer.get(errorBytes);
                    String errorString = new String(errorBytes);
                    logger.error("Failed to create topic: {}", errorString);
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            byte status = responseBuffer.get();
            boolean isSuccess = status == 0;

            if(isSuccess){
                //Refresh metadata to include new topic
                refreshMetaData();
            }

            return isSuccess;
        }
    }

    public Map<String, TopicMetaData> getTopicMetaData() {
        return new HashMap<>(this.topicMetaData);
    }

    public TopicMetaData getTopicMetaData(String topic) {
        return this.topicMetaData.get(topic);
    }

    public record TopicMetaData(String topic, List<PartitionInfo> partitions){
        @Override
        public List<PartitionInfo> partitions() {
            return new ArrayList<>(partitions);
        }

        @Override
        public String toString() {
            return "TopicMetaData{name: " + topic + ", partitions: " + partitions + "}";
        }
    }

    public record PartitionInfo(int id, int leader, List<Integer> followers){
        @Override
        public List<Integer> followers(){
            return new ArrayList<>(followers);
        }

        @Override
        public String toString() {
            return "PartitionInfo{id: " + id +  ", leader: " + leader + ", followers: " + followers + "}";
        }
    }
}
