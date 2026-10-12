package com.simplekafka.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleKafkaBroker {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaBroker.class);
    private static final String DATA_DIR = "data";

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private AtomicBoolean isRunning;
    private AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.zkClient = new ZookeeperClient("localhost", zkPort);

        File dataDir = new File(DATA_DIR);
        if(!dataDir.exists()){
            dataDir.mkdirs();
        }
    }

    /**
     * Main entry point for running a broker
     * @param args Arguments used while running the broker
     */
    public static void main(String args[]){
        if(args.length < 3){
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <brokerHost> <brokerPort> [zooKeeperPort]");
            System.exit(1);
        }

        try{
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

            System.out.println("SimpleKafka broker started. Press Ctrl + C to stop.");
        } catch (Exception e) {
            logger.error("Failed to start SimpleKafkaBroker.", e);
        }
    }

    /**
     * Start the broker server
     * @throws IOException Errors while setting up the server socket channel
     */
    public void start() throws IOException {
        if(this.isRunning.compareAndSet(false,true)) {
            // Bind to socket
            this.serverChannel.socket().bind(new InetSocketAddress(this.brokerHost, this.brokerPort));
            this.serverChannel.configureBlocking(false);
            logger.info("Started SimpleKafkaBroker at {}:{}", this.brokerHost, this.brokerPort);

            // Register with ZooKeeper
            registerWithZookeeper();

            // Start controller election
            electController();

            // Load existing topics
            loadTopics();

            // Accept client connections
            executor.submit(this::acceptConnections);
        }
    }

    /**
     * Stop the broker server
     */
    public void stop(){
        if(this.isRunning.compareAndSet(true,false)) {
            try{
                logger.info("Stopping SimpleKafkaBroker at {}:{}", this.brokerHost, this.brokerPort);
                this.serverChannel.close();

                for(List<Partition> partitions : this.topics.values()){
                    for(Partition partition : partitions){
                        partition.close();
                    }
                }

                this.executor.shutdown();
                this.executor.awaitTermination(5, TimeUnit.SECONDS);

                this.zkClient.close();
                logger.info("Stopped SimpleKafkaBroker at {}:{}", this.brokerHost, this.brokerPort);
            } catch (Exception e) {
                logger.error("Failed to stop SimpleKafkaBroker at {}:{}", this.brokerHost, this.brokerPort, e);
            }
        }
    }

    /**
     * Register this broker with ZooKeeper
     */
    private void registerWithZookeeper(){
        try {
            this.zkClient.connect();
            String brokerPath = "/brokers/" + this.brokerId;
            String brokerData = this.brokerHost + ":" + this.brokerPort;
            this.zkClient.createEphemeralNode(brokerPath, brokerData);

            BrokerInfo selfInfo = new BrokerInfo(this.brokerId, this.brokerHost, this.brokerPort);
            this.clusterMetadata.put(this.brokerId, selfInfo);

            this.zkClient.watchChildren("/brokers", this::onBrokerChange);
            logger.info("Registered with ZooKeeper at {}", this.zkClient.getConnectString());
        } catch(Exception e){
            logger.error("Failed to register broker at {}:{} with ZooKeeper", this.brokerHost, this.brokerPort, e);
        }
    }

    /**
     * Handle changes in the broker list from ZooKeeper
     * @param brokerIds
     */
    private void onBrokerChange(List<String> brokerIds){
        logger.info("Brokers change detected. Current brokers: {}", brokerIds);

        // Update cluster metadata
        for(String bId : brokerIds){
            try{
                int id =  Integer.parseInt(bId);
                if(!this.clusterMetadata.containsKey(id)){
                    String bData = this.zkClient.getData("/brokers/" + id);
                    String[] hostPort = bData.split(":");
                    BrokerInfo bInfo = new BrokerInfo(id, hostPort[0], Integer.parseInt(hostPort[1]));
                    this.clusterMetadata.put(id, bInfo);
                    logger.info("Added broker {}", bInfo);
                }
            } catch(Exception e){
                logger.warn("Failed to process broker info for broker {}", bId, e);
            }
        }

        // Remove the brokers that have disappeared
        List<Integer> toRemove = new ArrayList<>();
        for(Integer id : this.clusterMetadata.keySet()){
            if(!brokerIds.contains(String.valueOf(id))){
                toRemove.add(id);
            }
        }

        for(Integer id : toRemove){
            this.clusterMetadata.remove(id);
            logger.info("Removed broker {}", id);
        }

        // Rebalance cluster or re-elect controller if needed
        if(!brokerIds.contains(String.valueOf(this.brokerId)) && this.isController.get()){
            this.isController.set(false);
            logger.info("Broker {} is no longer in the cluster, giving up controller status", this.brokerId);
        }
        else if(this.isController.get()){
            // As controller, rebalance partitions due to cluster changes
            rebalancePartitions();
        }
        else{
            // Re-attempt controller election
            electController();
        }
    }

    /**
     * Participate in controller election
     */
    private void electController(){
        try{
            String controllerPath = "/controller";
            boolean nodeExists = this.zkClient.exists(controllerPath);
            if(nodeExists){
                String nodeData = this.zkClient.getData(controllerPath);
                if((nodeData == null) || nodeData.trim().isEmpty()){
                    this.zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                    logger.info("Deleted empty controller node");
                }
            }

            boolean becameController = false;
            if(!nodeExists){
                becameController = this.zkClient.createEphemeralNode(controllerPath, String.valueOf(this.brokerId));
            }

            if(becameController){
                logger.info("Elected controller is {}", this.brokerId);
                this.isController.set(true);
                rebalancePartitions();
            }
            else{
                String controllerId = this.zkClient.getData(controllerPath);
                if((controllerId == null) || controllerId.trim().isEmpty()){
                    logger.warn("Unexpected empty controller node");
                    new Thread(()->{
                        try{
                            Thread.sleep(100);
                            electController();
                        }
                        catch (InterruptedException e){
                            Thread.currentThread().interrupt();
                        }
                    }).start();
                    return;
                }
                logger.info("Current controller is {}", controllerId);
                this.zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            logger.error("Failed to elect controller", e);

            new Thread(()->{
                try{
                    Thread.sleep(100);
                    electController();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * Handle controller change notification
     */
    private void onControllerChange(){
        logger.info("Controller changed, initiating controller election");
        electController();
    }

    /**
     * Rebalance partitions across available brokers
     */
    private void rebalancePartitions(){
        if(!this.isController.get()){
            return;
        }

        for(Map.Entry<String, List<Partition>> entry : this.topics.entrySet()){
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            for(Partition partition : partitions){

                // Check if a partition doesn't have a leader
                if((partition.getLeader() == -1) || (!this.clusterMetadata.containsKey(partition.getLeader()))){
                    List<Integer> brokers = new ArrayList<>(this.clusterMetadata.keySet());
                    if(!brokers.isEmpty()){

                        // Assign a new leader
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);

                        // Assign followers
                        List<Integer> followers = new ArrayList<>();
                        for(int i = 1; i < Math.min(brokers.size(), 3); i++){
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);

                        // Update the partition metadata in ZooKeeper
                        updatePartitionMetadata(topic, partition);

                        logger.info("Reassigned partition {} of topic {} to leader {} and followers {}",
                                partition.getId(), topic, newLeader, followers);
                    }
                }
            }
        }
    }

    /**
     * Update partition metadata in ZooKeeper
     * @param topic Target topic
     * @param partition Target partition
     */
    private void updatePartitionMetadata(String topic, Partition partition){
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String followers = "";
            for (Integer follower : partition.getFollowers()) {
                followers += follower + ",";
            }
            String partitionData = partition.getLeader() + ";" + followers;

            // Create a node if it doesn't exist else update the data it holds
            this.zkClient.createPersistentNode(path, partitionData);

        } catch (Exception e) {
            logger.error("Failed to update metadata for partition {} of topic {}",  partition.getId(), topic, e);
        }
    }

    /**
     * Load all topics from ZooKeeper
     */
    private void loadTopics(){
        try{
            List<String> topics = this.zkClient.getChildren("/topics");
            for(String topic : topics){
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    logger.error("Failed to load topic: {}", topic, e);
                }
            }
            logger.info("Loaded {} topics", topics.size());
        } catch (Exception e) {
            logger.error("Failed to load topics", e);
        }
    }

    /**
     * Load topic metadata from ZooKeeper
     * @param topic Topic to be loaded
     * @throws Exception Topic doesn't exist in Zookeeper
     * OR Unable to get partition IDs from ZooKeeper
     * OR Unable to get partition data from ZooKeeper
     */
    private void loadTopic(String topic) throws Exception {
        if(this.topics.containsKey(topic)){
            logger.info("Topic {} is already loaded", topic);
            return;
        }

        String topicPath = "/topics/" + topic;
        if(!this.zkClient.exists(topicPath)){
            throw new Exception("Topic " + topic + " doesn't exist in ZooKeeper");
        }

        String topicDir = DATA_DIR + File.separator + this.brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<String> partitionIds = this.zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for(String partitionId : partitionIds){
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + id;
            String partitionData = this.zkClient.getData(partitionPath);

            String[] parts = partitionData.split(";");
            int leader =  Integer.parseInt(parts[0]);
            List<Integer> followers = new ArrayList<>();

            if((parts.length > 1) && (parts[1] != null)){
                String[] followerIds = parts[1].split(",");
                for(String followerId : followerIds){
                    if(!followerId.isEmpty()){
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            String partitionDir = topicDir + File.separator + partitionId;
            new File(partitionDir).mkdirs();

            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);
            logger.info("Loaded partition {} for topic {}, leader: {}, followers: {}", id, topic, leader, followers);
        }

        this.topics.put(topic, partitions);
        logger.info("Successfully loaded topic {} with {} partitions", topic, partitions.size());
    }

    /**
     * Accept client connections
     */
    private void acceptConnections(){
        while(this.isRunning.get()){
            try{
                SocketChannel clientChannel = this.serverChannel.accept();
                if(clientChannel != null){
                    clientChannel.configureBlocking(false);
                    logger.info("Accepted connections from {}", clientChannel.getRemoteAddress());
                    this.executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(200);
            } catch (Exception e) {
                if(this.isRunning.get()){
                    logger.error("Failed to accept connections", e);
                }
            }
        }
    }

    /**
     * Handle client connection
     * @param clientChannel Socket channel for communication with client
     */
    private void handleClient(SocketChannel clientChannel){
        try{
            while(clientChannel.isOpen() && this.isRunning.get()){
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                int bytesRead = clientChannel.read(buffer);
                if(bytesRead > 0){
                    buffer.flip();
                    processClientMessage(clientChannel, buffer);
                }
                else if (bytesRead < 0){
                    clientChannel.close();
                    break;
                }
            }
        } catch (Exception e) {
            if(this.isRunning.get()){
                logger.error("Failed to handle client", e);
            }
        } finally {
            try{
                if(clientChannel.isOpen()){
                    clientChannel.close();
                }
            } catch (IOException e) {
                logger.error("Failed to close client channel", e);
            }
        }
    }

    /**
     * Process client message based on SimpleKafka wire protocol
     * @param clientChannel Client communication socket channel
     * @param buffer ByteBuffer containing client message
     * @throws IOException Errors processing client message
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType){
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel);
                break;
            case Protocol.REPLICATE:
                handleReplicationRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotificationRequest(clientChannel, buffer);
                break;
            default:
                logger.warn("Unknown message type: {}", messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    /**
     * Handle produce request from client
     * @param clientChannel Client communication socket channel
     * @param buffer ByteBuffer containing client message
     * @throws IOException Errors writing messages to client socket channel
     * OR Errors writing messages to internal broker communication channels
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId =  buffer.getInt();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        logger.info("Produce request for topic {} and partition {}", topic, partitionId);

        if(!this.topics.containsKey(topic)){
            Protocol.sendErrorResponse(clientChannel, "Topic " + topic + " doesn't exist");
            return;
        }
        Partition targetPartition = null;
        for(Partition partition : this.topics.get(topic)){
            if(partition.getId() == partitionId){
                targetPartition = partition;
                break;
            }
        }
        if(targetPartition == null){
            Protocol.sendErrorResponse(clientChannel, "Partition " + partitionId + " doesn't exist");
            return;
        }

        if(targetPartition.getLeader() != this.brokerId){
            forwardProduceRequestToLeader(clientChannel, topic, partitionId, message, targetPartition.getLeader());
            return;
        }

        long msgOffset = targetPartition.append(message);

        replicateToFollowers(topic, targetPartition, message, msgOffset);

        ByteBuffer responseBuffer = ByteBuffer.allocate(10);
        responseBuffer.put(Protocol.PRODUCE_RESPONSE);
        responseBuffer.putLong(msgOffset);
        responseBuffer.put((byte) (msgOffset > -1 ? 0 : 1));
        responseBuffer.flip();
        clientChannel.write(responseBuffer);
    }

    /**
     * Forward produce request to partition leader
     * @param clientChannel Client communication socket channel
     * @param topic Target topic
     * @param partitionId Target partition ID
     * @param message Message to be produced
     * @param leaderId Partition leader ID
     * @throws IOException Errors sending error responses to client socket channel
     */
    private void forwardProduceRequestToLeader(SocketChannel clientChannel, String topic, int partitionId, byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = this.clusterMetadata.get(leaderId);
        if(leader == null){
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }
        try(SocketChannel leaderChannel = SocketChannel.open()){
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            ByteBuffer requestBuffer = Protocol.encodeProduceRequest(topic, partitionId, message);

            leaderChannel.write(requestBuffer);

            ByteBuffer responseBuffer = ByteBuffer.allocate(10);
            leaderChannel.read(responseBuffer);
            responseBuffer.flip();

            clientChannel.write(responseBuffer);
        } catch (Exception e) {
            logger.error("Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward produce request to leader");
        }
    }

    /**
     * Replicate message to follower brokers
     * @param topic Target topic
     * @param targetPartition Target partition
     * @param message Message to be replicated
     * @param msgOffset Target message offset
     */
    private void replicateToFollowers(String topic, Partition targetPartition, byte[] message, long msgOffset){
        List<Integer> followerIds = targetPartition.getFollowers();
        for(Integer followerId : followerIds){
            if(this.brokerId == followerId){
                continue;
            }
            BrokerInfo follower = this.clusterMetadata.get(followerId);
            if(follower == null){
                continue;
            }

            this.executor.submit(() ->{
                try(SocketChannel followerChannel = SocketChannel.open()){
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));

                    ByteBuffer requestBuffer = ByteBuffer.allocate(19 + topic.length() + message.length);
                    requestBuffer.put(Protocol.REPLICATE);
                    requestBuffer.putShort((short) topic.length());
                    requestBuffer.put(topic.getBytes());
                    requestBuffer.putInt(targetPartition.getId());
                    requestBuffer.putLong(msgOffset);
                    requestBuffer.putInt(message.length);
                    requestBuffer.put(message);
                    requestBuffer.flip();

                    followerChannel.write(requestBuffer);

                    ByteBuffer responseBuffer = ByteBuffer.allocate(1);
                    followerChannel.read(responseBuffer);
                    responseBuffer.flip();

                    byte ack = responseBuffer.get();
                    logger.info("Replication of partition {} to follower {} {}",
                            targetPartition.getId(), followerId, ack == Protocol.REPLICATE_ACK ? "SUCCEEDED" : "FAILED");
                } catch(Exception e) {
                    logger.error("Replication of partition {} to follower {} failed", targetPartition.getId(), followerId, e);
                }
            });
        }
    }

    /**
     * Handle fetch request from client
     * @param clientChannel Client communication socket channel
     * @param buffer ByteBuffer carrying the client message
     * @throws IOException Errors while writing messages to the client socket channel
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partitionId = buffer.getInt();
        long msgOffset =  buffer.getLong();
        int maxBytes = buffer.getInt();

        if(!this.topics.containsKey(topic)){
            Protocol.sendErrorResponse(clientChannel, "Topic " + topic + " doesn't exist");
            return;
        }

        List<Partition> partitions = this.topics.get(topic);
        Partition targetPartition = null;

        for(Partition partition : partitions){
            if(partition.getId() == partitionId){
                targetPartition = partition;
                break;
            }
        }

        if(targetPartition == null){
            Protocol.sendErrorResponse(clientChannel, "Partition " + partitionId + " doesn't exist");
            return;
        }

        if(msgOffset >= targetPartition.getLogEndOffset()){
            ByteBuffer responseBuffer = ByteBuffer.allocate(5);
            responseBuffer.put(Protocol.FETCH_RESPONSE);
            responseBuffer.putInt(0);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
            return;
        }

        List<byte[]> messages = targetPartition.readMessages(msgOffset, maxBytes);
        int bufferSize = 5;
        for(byte[] message : messages){
            bufferSize += 12 + message.length;
        }
        ByteBuffer responseBuffer = ByteBuffer.allocate(bufferSize);
        responseBuffer.put(Protocol.FETCH_RESPONSE);
        responseBuffer.putInt(messages.size());


        long currentOffset = msgOffset;
        for(byte[] message : messages){
            responseBuffer.putLong(currentOffset);
            responseBuffer.putInt(message.length);
            responseBuffer.put(message);
            currentOffset++;
        }
        responseBuffer.flip();
        clientChannel.write(responseBuffer);
    }

    /**
     * Handle metadata request from client
     * @param clientChannel Client communication socket channel
     * @throws IOException Errors while writing messages to the client socket channel
     */
    private void handleMetadataRequest(SocketChannel clientChannel) throws IOException {
        int responseBufferSize = 5;
        for(Map.Entry<Integer, BrokerInfo> entry : this.clusterMetadata.entrySet()){
            BrokerInfo brokerInfo = entry.getValue();
            responseBufferSize += 10 + brokerInfo.getHost().length();
        }

        responseBufferSize += 4;
        for(Map.Entry<String, List<Partition>> entry : this.topics.entrySet()){
            responseBufferSize += 6 + entry.getKey().length();
            for(Partition partition : entry.getValue()){
                responseBufferSize += 12 + (4 * partition.getFollowers().size());
            }
        }

        ByteBuffer responseBuffer = ByteBuffer.allocate(responseBufferSize);
        responseBuffer.put(Protocol.METADATA_RESPONSE);

        // Add broker metadata
        responseBuffer.putInt(this.clusterMetadata.size());
        for(BrokerInfo brokerInfo : this.clusterMetadata.values()){
            responseBuffer.putInt(brokerInfo.getId());
            responseBuffer.putShort((short) brokerInfo.getHost().length());
            responseBuffer.put(brokerInfo.getHost().getBytes());
            responseBuffer.putInt(brokerInfo.getPort());
        }

        // Add topic metadata
        responseBuffer.putInt(this.topics.size());
        for(Map.Entry<String, List<Partition>> entry : this.topics.entrySet()){
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            responseBuffer.putShort((short) topic.length());
            responseBuffer.put(topic.getBytes());

            responseBuffer.putInt(partitions.size());
            for(Partition partition : partitions){
                responseBuffer.putInt(partition.getId());
                responseBuffer.putInt(partition.getLeader());

                responseBuffer.putInt(partition.getFollowers().size());
                for(Integer followerId : partition.getFollowers()) {
                    responseBuffer.putInt(followerId);
                }
            }
        }

        responseBuffer.flip();
        clientChannel.write(responseBuffer);
    }

    /**
     * Handle replication request from partition leader
     * @param clientChannel Client (here, partition leader) communication socket channel
     * @param buffer ByteBuffer containing the client message
     * @throws IOException Errors while writing messages to the client socket channel
     */
    private void    handleReplicationRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long msgOffset = buffer.getLong();
        int msgLength = buffer.getInt();
        byte[] message = new byte[msgLength];
        buffer.get(message);

        if(!this.topics.containsKey(topic)){
            ByteBuffer responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put((byte) 0);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
            return;
        }

        List<Partition> partitions = this.topics.get(topic);
        Partition targetPartition = null;
        for(Partition partition : partitions){
            if(partition.getId() == partitionId){
                targetPartition = partition;
                break;
            }
        }

        if(targetPartition == null){
            ByteBuffer responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put((byte) 0);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
            return;
        }

        if(targetPartition.getLogEndOffset() != msgOffset){
            //TODO: Repair the log
        }
        targetPartition.append(message);
        ByteBuffer responseBuffer = ByteBuffer.allocate(1);
        responseBuffer.put(Protocol.REPLICATE_ACK);
        responseBuffer.flip();
        clientChannel.write(responseBuffer);
    }

    /**
     * Handle create topic request from client
     * @param clientChannel Client communication socket channel
     * @param buffer ByteBuffer containing the client message
     * @throws IOException Errors while writing messages on the client socket channel
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if(this.topics.containsKey(topic)){
            logger.error("Topic {} already exists", topic);
            Protocol.sendErrorResponse(clientChannel, "Topic " + topic + " already exists");
            return;
        }

        if(numPartitions <= 0 || replicationFactor <= 0 || replicationFactor > clusterMetadata.size()){
            logger.error("Invalid topic configuration parameters, numPartitions: {}, replicationFactor: {}", numPartitions, replicationFactor);
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }
        if(!this.isController.get()){
            forwardTopicCreationToController(topic, numPartitions, replicationFactor, clientChannel);
            return;
        }

        createTopic(topic, numPartitions, replicationFactor);
        ByteBuffer responseBuffer = ByteBuffer.allocate(2);
        responseBuffer.put(Protocol.CREATE_TOPIC_RESPONSE);
        responseBuffer.put((byte) 0);
        responseBuffer.flip();
        clientChannel.write(responseBuffer);
    }

    /**
     * Forward create topic request to controller
     * @param topic Topic to be created
     * @param numPartitions Number of partitions in the topic
     * @param replicationFactor Replication factor for partitions in the topic
     * @param clientChannel Client communication socket channel
     * @throws IOException Errors while sending error messages on the client channel
     */
    private void forwardTopicCreationToController(String topic, int numPartitions, short replicationFactor, SocketChannel clientChannel) throws IOException {
        int controllerId = -1;
        try{
            String controllerData = this.zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e){
            logger.error("Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = this.clusterMetadata.get(controllerId);
        if(controller == null){
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        try(SocketChannel controllerChannel = SocketChannel.open()){
            controllerChannel.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));
            ByteBuffer requestBuffer = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);

            controllerChannel.write(requestBuffer);

            ByteBuffer responseBuffer = ByteBuffer.allocate(2);
            controllerChannel.read(responseBuffer);
            responseBuffer.flip();

            clientChannel.write(responseBuffer);
        } catch (Exception e) {
            logger.error("Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward request to controller");
        }
    }

    /**
     * Create a new topic with the specified configuration
     * @param topic Topic to be created
     * @param numPartitions Number of partitions for the topic
     * @param replicationFactor Replication factor for the partitions in the topic
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if(!this.isController.get()){
            logger.warn("Only controller can create topics");
            return;
        }

        try{
            // Create topic directory
            String topicDir = DATA_DIR + File.separator + topic;
            new File(topicDir).mkdirs();

            // Create topic in ZooKeeper
            String topicPath = "/topics/" + topic;
            if(!this.zkClient.exists(topicPath)){
                this.zkClient.createPersistentNode(topicPath, "");
                this.zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

            // Create partitions
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(this.clusterMetadata.keySet());

            for(int i = 0; i < numPartitions; i++){
                int partitionId = i;

                // Create partition directory
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();

                // Select leader
                int leaderIndex =  i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);
                List<Integer> followers = new ArrayList<>();

                // Select followers
                for(int j = 1; j < replicationFactor; j++){
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    int followerId =  brokerIds.get(followerIndex);
                    followers.add(followerId);
                }

                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);

                // Store partition metadata in ZooKeeper
                String partitionPath = topicPath + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for(Integer follower : followers){
                    partitionData += follower + ",";
                }

                this.zkClient.createPersistentNode(partitionPath, partitionData);
                logger.info("Created partition {} for topic {} with leader {} and followers {}",
                        partitionId, topic, leaderId, followers);
            }

            // Add topic to broker's metadata
            this.topics.put(topic, partitions);

            // Notify all brokers to store the topic
            for(Integer bId : brokerIds){
                if(bId != this.brokerId){
                    notifyBrokerForTopicCreation(bId, topic);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to create topic", e);
        }
    }

    /**
     * Notify a broker about topic creation
     * @param brokerId ID of the broker that is to be notified
     * @param topic Topic about which the target broker is to be notified
     */
    private void notifyBrokerForTopicCreation(Integer brokerId, String topic) {
        BrokerInfo brokerInfo = this.clusterMetadata.get(brokerId);
        if(brokerInfo == null){
            return;
        }

        this.executor.submit(() -> {
            try(SocketChannel brokerChannel = SocketChannel.open()){
                brokerChannel.connect(new InetSocketAddress(brokerInfo.getHost(), brokerInfo.getPort()));

                ByteBuffer requestBuffer = Protocol.encodeTopicNotification(topic);
                brokerChannel.write(requestBuffer);

                ByteBuffer responseBuffer = ByteBuffer.allocate(1);
                brokerChannel.read(responseBuffer);
                responseBuffer.flip();
            } catch (Exception e) {
                logger.error("Failed to notify broker {} about creation of topic {}", brokerId, topic, e);
            }
        });
    }

    /**
     * Handle topic notification request coming from the controller
     * @param clientChannel Channel that will be used to send back the response
     * @param buffer ByteBuffer containing input values
     * @throws IOException Unable to write response to the client channel
     */
    private void handleTopicNotificationRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        try{
            loadTopic(topic);
            ByteBuffer responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put((byte) 0);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
        } catch (Exception e) {
            logger.error("Failed to load topic {}", topic, e);
            ByteBuffer responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put((byte) 1);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
        }
    }
}
