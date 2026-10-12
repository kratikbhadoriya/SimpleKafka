package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the wire protocol for SimpleKafka
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x15;

    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    /*
     --------------------------------------------------------------------------------------------------------------
     |                                                   CLASSES                                                  |
     --------------------------------------------------------------------------------------------------------------
     */

    /**
     * Partition metadata record class
     */
    public record PartitionMetaData(int id, int leadBrokerId, List<Integer> replicasBrokerIds) {}

    /**
     * Topic metadata record class
     */
    public record TopicMetaData(String topic, List<PartitionMetaData> partitions) {
    }

    /**
     * Result record class for produce response
     */
    public record ProduceResult(long offset, String error) {
        public boolean isSuccess() {
            return this.error == null;
        }
    }

    /**
     * Result record class for fetch response
     */
    public record FetchResult(byte[][] messages, String error) {
        public Integer getNumMessages() {
            if (messages == null) return null;
            return messages.length;
        }

        public boolean isSuccess() {
            return this.error == null;
        }
    }

    /**
     * Result class for metadata response
     */
    public record MetadataResult(List<BrokerInfo> brokers, List<TopicMetaData> topics, String error) {
        public boolean isSuccess() {
            return this.error == null;
        }
    }

    /*
     --------------------------------------------------------------------------------------------------------------
     |                                                   REQUESTS                                                 |
     --------------------------------------------------------------------------------------------------------------
     */

    /**
     * Encode a produce request
     * @param topic Topic name
     * @param partitionId Partition ID
     * @param message Message byte array
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partitionId, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partitionId);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a fetch request
     * @param topic Topic name
     * @param partitionId Partition ID
     * @param offset Offset of the message in the partition
     * @param maxBytes Maximum bytes to fetch
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partitionId, long offset, int maxBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partitionId);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a metadata request
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a create topic request
     * @param topic Topic name
     * @param numPartitions Number of partitions in the topic
     * @param replicationFactor Replication factor for the messages to be persisted
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a replication request
     * @param topic Topic name
     * @param partitionId Partition ID
     * @param offset Offset of the message in the partition
     * @param message Message byte array
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partitionId, long offset, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length() + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partitionId);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a topic notification request
     * @param topic Topic name
     * @return ByteBuffer object
     */
    public static ByteBuffer encodeTopicNotification(String topic){
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }

    /*
     --------------------------------------------------------------------------------------------------------------
     |                                                  RESPONSES                                                 |
     --------------------------------------------------------------------------------------------------------------
     */

    /**
     * Send an error response to the client
     * @param socketChannel Socket channel object used for communication with the client
     * @param errorMessage Error message to be sent
     * @throws IOException IO exception
     */
    public static void sendErrorResponse(SocketChannel socketChannel, String errorMessage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();

        socketChannel.write(buffer);
    }

    /**
     * Decode produce response
     * @param buffer ByteBuffer object containing the produce response
     * @return ProduceResult object
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if(responseType != PRODUCE_RESPONSE){
            if(responseType == ERROR_RESPONSE){
                short errorLength =  buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }
        long offset = buffer.getLong();
        byte status = buffer.get();
        return new ProduceResult(offset, status == 0 ? null : "Produce Failed");
    }

    /**
     * Decode fetch response
     * @param buffer ByteBuffer object containing the fetch response
     * @return FetchResult object
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if(responseType != FETCH_RESPONSE){
            if(responseType == ERROR_RESPONSE){
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }
        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        for(int i = 0; i < messageCount; i++){
            long offset = buffer.getLong();
            int messageLength = buffer.getInt();
            messages[i] = new byte[messageLength];
            buffer.get(messages[i]);
        }
        return new FetchResult(messages, null);
    }

    /**
     * Decode metadata response
     * @param buffer ByteBuffer object containing the metadata response
     * @return MetadataResult object
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if(responseType != METADATA_RESPONSE){
            if(responseType == ERROR_RESPONSE){
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(null, null, error);
            }
            return new MetadataResult(null, null, "Invalid response type");
        }

        // Parse broker metadata
        int numBrokers = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for(int i = 0; i < numBrokers; i++){
            int id = buffer.getInt();
            short hostAddressLength =  buffer.getShort();
            byte[] hostAddressBytes = new byte[hostAddressLength];
            buffer.get(hostAddressBytes);
            String host = new String(hostAddressBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, host, port));
        }

        // Parse topic metadata
        int numTopics = buffer.getInt();
        List<TopicMetaData> topics = new ArrayList<>();
        for(int i = 0; i < numTopics; i++){
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topic = new String(topicBytes);

            // Parse partition metadata
            int numPartitions = buffer.getInt();
            List<PartitionMetaData> partitions = new ArrayList<>();
            for(int j = 0; j < numPartitions; j++){
                int id = buffer.getInt();
                int leadBrokerId = buffer.getInt();

                int numReplicas = buffer.getInt();
                List<Integer> replicaBrokerIds = new ArrayList<>();
                for(int k = 0; k < numReplicas; k++){
                    Integer replicaId = buffer.getInt();
                    replicaBrokerIds.add(replicaId);
                }
                partitions.add(new PartitionMetaData(id, leadBrokerId, replicaBrokerIds));
            }
            topics.add(new TopicMetaData(topic, partitions));
        }
        return new MetadataResult(brokers, topics, null);
    }
}
