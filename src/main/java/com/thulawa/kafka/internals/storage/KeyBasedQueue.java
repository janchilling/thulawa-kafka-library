package com.thulawa.kafka.internals.storage;

import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.apache.kafka.streams.processor.api.Record;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * KeyBasedQueue is a Queue class that provides a thread-safe, key-based queue for storing and managing Kafka Records.
 * It uses a ConcurrentLinkedQueue to ensure safe concurrent access by multiple threads.
 * i.e. Have to optimize more, should focus on other queue methods and interfacing. As of writing this, this is the
 * initial implementation.
 *
 * Better to take Metrics to a map in QueueManager!!!!
 */
public class KeyBasedQueue {

    private final String recordKey;

    private final ConcurrentLinkedQueue<Record> recordsQueue;
//    private ThulawaMetricsRecorder thulawaMetricsRecorder;

    /**
     * Constructor for the KeyBasedQueue class.
     * Initializes the queue and sets the key for this queue.
     *
     * @param recordKey The key associated with this queue.
     */
    public KeyBasedQueue(String recordKey) {
        this.recordsQueue = new ConcurrentLinkedQueue<>();
        this.recordKey = recordKey;
    }

    /**
     * Adds a new Kafka Record to the queue.
     *
     * @param record The Kafka Record to add to the queue.
     */
    public void add(Record record) {
        recordsQueue.offer(record);
//        this.thulawaMetricsRecorder.updateKeyBasedQueueSizes(this.size());
    }

    /**
     * Retrieves and removes the head of the queue, or returns null if the queue is empty.
     *
     * @return The head Kafka Record, or null if the queue is empty.
     */
    public Record<Object, Object> poll() {
        return recordsQueue.poll();
    }

    /**
     * Returns the number of records currently in the queue.
     *
     * @return The size of the queue.
     */
    public int size() {
        return recordsQueue.size();
    }

    /**
     * Checks whether the queue is empty.
     *
     * @return true if the queue is empty, false otherwise.
     */
    public boolean isEmpty() {
        return recordsQueue.isEmpty();
    }

    /**
     * Clears all the records from the queue.
     */
    public void clear() {
        recordsQueue.clear();
    }
}
