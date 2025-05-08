package com.thulawa.kafka.internals.storage;

import com.thulawa.kafka.ThulawaEvent;

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

    private final Object recordKey;

    private final ConcurrentLinkedQueue<ThulawaEvent> recordsQueue;
//    private ThulawaMetricsRecorder thulawaMetricsRecorder;

    /**
     * Constructor for the KeyBasedQueue class.
     * Initializes the queue and sets the key for this queue.
     *
     * @param recordKey The key associated with this queue.
     */
    public KeyBasedQueue(Object recordKey) {
        this.recordsQueue = new ConcurrentLinkedQueue<>();
        this.recordKey = recordKey;
    }

    /**
     * Adds a new Kafka Record to the queue.
     *
     * @param thulawaEvent The Kafka Record to add to the queue.
     */
    public void add(ThulawaEvent thulawaEvent) {
        recordsQueue.offer(thulawaEvent);
//        this.thulawaMetricsRecorder.updateKeyBasedQueueSizes(this.size());
    }

    public long peekTimestamp() {
        ThulawaEvent event = recordsQueue.peek();
        return (event != null) ? event.getReceivedSystemTime() : Long.MAX_VALUE;
    }


    /**
     * Retrieves and removes the head of the queue, or returns null if the queue is empty.
     *
     * @return The head Kafka Record, or null if the queue is empty.
     */
    public ThulawaEvent poll() {
        return recordsQueue.poll();
    }

    public ThulawaEvent peek() {
        return recordsQueue.peek();
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
