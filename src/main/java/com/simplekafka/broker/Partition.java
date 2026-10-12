package com.simplekafka.broker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents the message storage layer for the SimpleKafka message broker
 */
public class Partition {
    private static final Logger logger = LoggerFactory.getLogger(Partition.class);
    private static final int MAX_SEGMENT_SIZE = 1024 * 1024;
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    @Getter
    private final int id;

    @Getter
    @Setter
    private int leader;

    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private List<SegmentInfo> segments;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new  ArrayList<>();
        
        initialise();
    }

    /**
     * Initialises the partition for log operations
     */
    private void initialise() {
        try {
            // Create directory if it does not exist
            File dir = new File(this.baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Load existing segments
            File[] files = dir.listFiles(
                    (dir1, name) -> name.endsWith(LOG_SUFFIX)
            );
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    File indexFile = new File(this.baseDir, baseName + INDEX_SUFFIX);
                    long baseOffset = Long.parseLong(baseName);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getPath(), indexFile.getPath());
                        this.segments.add(segment);
                    }
                }

                // Sort existing segments
                this.segments.sort((s1, s2) -> Long.compare(s1.baseOffset(), s2.baseOffset()));

                // Determine next available offset
                if (!this.segments.isEmpty()) {
                    SegmentInfo lastSegment = this.segments.get(this.segments.size() - 1);
                    this.nextOffset.set(lastSegment.baseOffset() + this.countMessagesInSegment(lastSegment));
                }
            }

            if (this.segments.isEmpty()) {
                createNewSegment(0);
            } else {
                SegmentInfo lastSegment = this.segments.get(this.segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }
        }
        catch(Exception e){
            logger.error("Failed to initialise partition {}", this.id, e);
        }
    }

    /**
     * Counts the number of messages in a segment
     * @param segment Target segment information
     * @return Count of messages in the segment
     * @throws IOException
     */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long msgCount = 0;
        try(RandomAccessFile logFile = new RandomAccessFile(segment.logPath(), "r");
        FileChannel logChannel = logFile.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            while(logChannel.position() < logChannel.size()) {
                // Prepare the buffer for reading message size
                buffer.clear();

                // Write the message size from log channel into buffer
                int bytesRead = logChannel.read(buffer);
                if(bytesRead < 4) break;

                // read the message size
                buffer.flip();
                int msgSize = buffer.getInt();

                // Skip the message
                logChannel.position(logChannel.position() + msgSize);
                msgCount++;
            }
        }
        return msgCount;
    }

    /**
     * Creates a new segment for the given base offset
     * @param baseOffset Base offset of the segment
     * @throws IOException
     */
    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = this.baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = this.baseDir + File.separator + baseName + INDEX_SUFFIX;

        // Create a log file
        File logFile = new File(logPath);
        logFile.createNewFile();

        // Create an index file
        File indexFile = new File(indexPath);
        indexFile.createNewFile();

        // Create a new segment
        SegmentInfo segment = new SegmentInfo(baseOffset, logFile.getPath(), indexFile.getPath());
        this.segments.add(segment);

        // Open the segment for append
        openSegmentForAppend(segment);
    }

    /**
     * Opens the segment for appending new messages
     * @param segment Target segment information
     * @throws IOException
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close the currently active segment if any
        if(this.activeLogChannel != null &&  this.activeLogChannel.isOpen()){
            this.activeLogChannel.close();
        }

        if(this.activeLogFile != null){
            this.activeLogFile.close();
        }

        // Open a new segment
        this.activeLogFile = new RandomAccessFile(segment.logPath(),"rw");
        this.activeLogChannel = this.activeLogFile.getChannel();

        // Move to the end of the file to operate in append mode
        this.activeLogChannel.position(this.activeLogChannel.size());
    }

    /**
     * Appends the message to the active segment
     * @param msg Byte message array
     * @return Message offset
     */
    public long append(byte[] msg){
        // Acquire write lock
        this.lock.writeLock().lock();
        try {
            // Get current offset
            long msgOffset = this.nextOffset.get();

            // Check if segment size, create a new one if full
            if (this.activeLogChannel.position() >= MAX_SEGMENT_SIZE) {
                this.activeLogChannel.close();
                this.activeLogFile.close();
                createNewSegment(msgOffset);
            }

            // write the message into the buffer
            ByteBuffer buffer = ByteBuffer.allocate(4 + msg.length);
            buffer.putInt(msg.length);
            buffer.put(msg);
            buffer.flip();

            // write the message into file
            long msgPosition = this.activeLogChannel.position();
            this.activeLogChannel.write(buffer);

            // write message to disk
            this.activeLogChannel.force(true);

            // Update index
            updateIndex(msgOffset, msgPosition);

            // Increment offset
            this.nextOffset.incrementAndGet();

            return msgOffset;
        } catch (IOException e) {
            logger.error("Failed to append message to partition {}", this.id, e);
            return -1;
        }
        finally {
            // Release the write lock
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Append new message offset and position pair to index
     * @param msgOffset Message offset
     * @param msgPosition Message position
     */
    private void updateIndex(long msgOffset, long msgPosition){
        try{
            if(this.segments.isEmpty()){
                return;
            }

            SegmentInfo  lastSegment = this.segments.get(this.segments.size() - 1);
            RandomAccessFile indexFile = new RandomAccessFile(lastSegment.indexPath(), "rw");
            FileChannel  indexChannel = indexFile.getChannel();
            indexChannel.position(indexChannel.size());

            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(msgOffset);
            buffer.putLong(msgPosition);
            buffer.flip();

            indexChannel.write(buffer);
            indexChannel.force(true);
        } catch (Exception e) {
            logger.error("Failed to update index for message with offset {} in partition {}", msgOffset, this.id, e);
        }
    }

    /**
     * Reads messages until the bytes read limit
     * @param offset Starting target message offset
     * @param maxBytes Bytes read limit
     * @return List of messages read
     */
    public List<byte[]> readMessages(long offset, int maxBytes){
        this.lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;

        try{
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if(targetSegment == null){
                return messages;
            }
            long msgPosition = findPositionForOffset(targetSegment, offset);
            if(msgPosition < 0){
                return messages;
            }

            long currentOffset = offset;

            try{
                RandomAccessFile logFile = new RandomAccessFile(targetSegment.logPath(), "r");
                FileChannel logChannel = logFile.getChannel();
                logChannel.position(msgPosition);

                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

                while((bytesRead < maxBytes) && (logChannel.position() < logChannel.size())) {
                    sizeBuffer.clear();
                    int sizeRead = logChannel.read(sizeBuffer);
                    if(sizeRead < 4) break;
                    sizeBuffer.flip();

                    int msgSize = sizeBuffer.getInt();
                    if(bytesRead + 4 + msgSize > maxBytes) {
                        break;
                    }
                    ByteBuffer msgBuffer = ByteBuffer.allocate(msgSize);
                    int msgRead = logChannel.read(msgBuffer);
                    if(msgRead < msgSize) {
                        logger.warn("Incomplete message read for offset {}", currentOffset);
                        break;
                    }
                    msgBuffer.flip();

                    byte[] msg = new byte[msgSize];
                    msgBuffer.get(msg);
                    messages.add(msg);

                    bytesRead += msgSize + 4;
                    currentOffset++;

                    if((logChannel.position() < logChannel.size()) && (currentOffset < this.nextOffset.get())){
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        logChannel.close();
                        logFile.close();

                        targetSegment = segments.get(nextSegmentIndex);
                        logFile = new RandomAccessFile(targetSegment.logPath(), "r");
                        logChannel = logFile.getChannel();

                    }
                }
            }catch(IOException e){
                logger.error("Failed to read message with offset {} from the segment {} of partition {}",
                        currentOffset,  targetSegment, this.id, e);
            }

        } catch (Exception e){
            logger.error("Failed to read messages from partition {}", this.id, e);
        } finally {
            this.lock.readLock().unlock();
        }

        return messages;
    }

    /**
     * Finds the segment containing target offset
     * @param offset Target message offset
     * @return Target segment information
     */
    private SegmentInfo findSegmentForOffset(long offset){
        if((this.segments.isEmpty()) || (offset >= nextOffset.get())){
            return null;
        }
        int low =  0;
        int high = this.segments.size() - 1;
        while(low <= high){
            int mid = low + (high - low) / 2;
            SegmentInfo segment = this.segments.get(mid);

            if(mid < this.segments.size() - 1){
                SegmentInfo nextSegment = this.segments.get(mid + 1);
                if((offset >= segment.baseOffset()) && (offset < nextSegment.baseOffset())){
                    return segment;
                }
                else if (offset < segment.baseOffset()) {
                    high = mid - 1;
                }
                else {
                    low = mid + 1;
                }
            }
            else{
                if(offset >= segment.baseOffset()){
                    return segment;
                }
            }
        }
        return null;
    }

    /**
     * Finds message position in segment for the target offset
     * @param segment Target segment information
     * @param offset Target message offset
     * @return
     */
    long findPositionForOffset(SegmentInfo segment, long offset){
        try(RandomAccessFile indexFile = new RandomAccessFile(segment.indexPath(), "r");
        FileChannel indexChannel = indexFile.getChannel()){

            // Empty index, start from beginning of the segment
            if(indexChannel.size() == 0){
                return 0;
            }

            // Relative offset within the segment
            long relativeOffset = offset - segment.baseOffset();

            long totalIndices = indexChannel.size() / 16;

            // If offset not found in index, select the last known
            if(relativeOffset >= totalIndices){
                indexChannel.position(indexChannel.size() - 16);

                // Read the last known offset and position
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                buffer.flip();

                // Ignore offset
                buffer.getLong();

                // Return position
                return buffer.getLong();
            }

            // Move to the position corresponding to target offset
            indexChannel.position(relativeOffset * 16);

            // Read the offset and position
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();

            // Ignore offset
            buffer.getLong();

            // Return position
            return buffer.getLong();
        } catch(IOException e){
            logger.error("Failed to find position for offset {}", offset, e);
            return -1;
        }
    }

    /**
     * Closes the partition resources
     */
    public void close(){
        this.lock.writeLock().lock();
        try{
            if((this.activeLogChannel != null) && (this.activeLogChannel.isOpen())){
                this.activeLogChannel.close();
            }
            if(this.activeLogFile != null){
                this.activeLogFile.close();
            }
        } catch(IOException e){
            logger.error("Failed to close resources for partition {}", this.id, e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Returns a copy of the list of followers for the partition
     * @return Copy of the list of followers for the partition
     */
    public List<Integer> getFollowers(){
        // Defensive copying to preserve the internal partition state
        return new ArrayList<>(this.followers);
    }

    /**
     * Sets the list of followers for the partition
     * @param followers List of followers for the partition
     */
    public void setFollowers(List<Integer> followers){
        // Defensive copying to preserve the internal partition state
        this.followers = new ArrayList<>(followers);
    }

    /**
     * Gets the current log end offset
     * @return Current log end offset
     */
    public long getLogEndOffset(){
        return this.nextOffset.get();
    }

    /**
     * Data carrier for carrying segment information within the partition class
     * @param baseOffset Base message offest of the segment
     * @param logPath Path of the .log file corresponding to the segment
     * @param indexPath Path of the .index file corresponding to the segment
     */
    private record SegmentInfo(long baseOffset, String logPath, String indexPath) {
    }
}
