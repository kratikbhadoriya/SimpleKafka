package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ZookeeperClient implements Watcher {
    private final int SESSION_TIMEOUT = 30000;
    private final Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
    private final ExecutorService reconnectExecutor = Executors.newSingleThreadExecutor();

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private String host;
    private int port;

    public ZookeeperClient(String host, int port){
        this.host = host;
        this.port = port;
    }

    public interface ChildrenCallback {
        void onChildrenChange(List<String> children);
    }

    public interface NodeCallback{
        void onNodeChange();
    }

    public void connect() throws IOException, InterruptedException {
        this.zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        //creating the required paths once a new connection is established
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    public String getConnectString(){
        return this.host + ":" + this.port;
    }

    public void close() throws InterruptedException {
        this.reconnectExecutor.shutdown();
        if(this.zooKeeper != null){
            this.zooKeeper.close();
        }
    }

    public boolean exists(String path) throws InterruptedException, KeeperException {
        Stat stat = this.zooKeeper.exists(path, false);
        return stat != null;
    }

    public void createPersistentNode(String path, String data) throws InterruptedException, KeeperException {
        if (!this.exists(path)) {
            this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            this.logger.info("Created persistent node: {}", path);
        }
        else{
            this.zooKeeper.setData(path, data.getBytes(), -1);
            this.logger.info("Updated persistent node: {}", path);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws InterruptedException, KeeperException {
        if (!this.exists(path)) {
            this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            this.logger.info("Created ephemeral node: {}", path);
            return true;
        }
        else{
            this.logger.info("The ephemeral node already exists: {}", path);
            return false;
        }
    }

    public void deleteNode(String path) throws InterruptedException, KeeperException {
        if(this.exists(path)){
            this.zooKeeper.delete(path, -1);
        }
    }

    public String getData(String path) throws InterruptedException, KeeperException {
        byte[] data = this.zooKeeper.getData(path, false, null);
        return new String(data);
    }

    public void setData(String path, String data) throws InterruptedException, KeeperException {
        this.zooKeeper.setData(path, data.getBytes(), -1);
    }

    public List<String> getChildren(String path) throws InterruptedException, KeeperException {
        try{
            return this.zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }
    public void createPath(String path){
        try{
            if(path.equals("/")){
                return;
            }
            if(path.lastIndexOf('/') > 0){
                String parentPath = path.substring(0, path.lastIndexOf('/'));
                createPath(parentPath);
            }
            if(!this.exists(path)){
                this.zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                this.logger.info("Created path: {}", path);
            }
        } catch (Exception e) {
            this.logger.warn("Failed to create path: {}", path, e);
        }
    }

    public void watchChildren(String path, ChildrenCallback callBack){
        try{
            List<String> children = this.zooKeeper.getChildren(path, event -> {
               if(event.getType() == Event.EventType.NodeChildrenChanged){
                   try{
                       List<String> newChildren = this.zooKeeper.getChildren(path, newEvent -> {
                           if(event.getType() == Event.EventType.NodeChildrenChanged){
                               watchChildren(path, callBack);
                           }
                       });
                       callBack.onChildrenChange(newChildren);
                   }
                   catch (Exception e){
                       this.logger.warn("Failed to get the updated children for the path: {}", path, e);
                   }
               }
            });
            callBack.onChildrenChange(children);
        }
        catch (Exception e){
            this.logger.warn("Failed to watch children for the path: {}", path, e);
        }
    }

    public void watchNode(String path, NodeCallback callBack){
        try{
            this.zooKeeper.exists(path, event -> {
               if((event.getType() == Event.EventType.NodeCreated) ||
                       (event.getType() == Event.EventType.NodeDataChanged) ||
                       (event.getType() == Event.EventType.NodeDeleted)){
                   callBack.onNodeChange();
               }
            });
        }
        catch (Exception e){
            this.logger.warn("Failed to watch the node {}", path, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
            this.logger.info("Connected to the ZooKeeper");
        }
        else if(event.getState() == Event.KeeperState.Disconnected){
            this.logger.info("Disconnected from the ZooKeeper");
        }
        else if(event.getState() == Event.KeeperState.Expired){
            this.logger.info("ZooKeeper session has expired, reconnecting....");
            try{
                if(this.zooKeeper != null){
                    this.zooKeeper.close();
                }
                this.connectedSignal = new  CountDownLatch(1);
                this.reconnectExecutor.submit(() -> {
                    try{
                        this.connect();
                    }
                    catch (Exception e) {
                        if(e instanceof InterruptedException){
                            Thread.currentThread().interrupt();
                        }
                        this.logger.warn("Failed to reconnect to the ZooKeeper", e);
                    }
                });
            }
            catch (Exception e){
                this.logger.warn("Failed to reconnect to ZooKeeper", e);
            }
        }
    }
}
