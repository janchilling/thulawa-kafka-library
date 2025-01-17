package com.thulawa.kafka.internals.helpers;

import com.thulawa.kafka.internals.storage.KeyBasedQueue;
import com.thulawa.kafka.scheduler.Scheduler;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueueManager dynamically manages key-based queues for storing records.
 */
public class QueueManager {

    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);

    private static QueueManager instance;

    private static final String NULL_KEY_QUEUE = "__NULL_KEY__";

    // Map to hold queues for each key
    private final Map<String, KeyBasedQueue> queues = new ConcurrentHashMap<>();

    // Map to track the size of each queue
    private final Map<String, Integer> queueSizeMap = new ConcurrentHashMap<>();

    private Scheduler schedulerObserver;

    private QueueManager() {
    }

    public static synchronized QueueManager getInstance() {
        if (instance == null) {
            instance = new QueueManager();
        }
        return instance;
    }

    /**
     * Adds a record to the queue associated with the given key.
     * If the queue doesn't exist, it is created.
     *
     * @param key    The key for the queue (nullable).
     * @param record The record to add.
     */
    public void addToKeyBasedQueue(String key, Record record) {
        String queueKey = key == null ? NULL_KEY_QUEUE : key;
        queues.computeIfAbsent(queueKey, k -> new KeyBasedQueue(key));
        queues.get(queueKey).add(record);
        updateQueueSizeMap(queueKey);

        // Notify observer if scheduler is not ACTIVE
        if (schedulerObserver != null && !schedulerObserver.isActive()) {
            schedulerObserver.notifyScheduler();
        }
    }

    /**
     * Retrieves and removes a record from the queue associated with the given key.
     *
     * @param key The key for the queue.
     * @return The retrieved record, or null if the queue is empty or doesn't exist.
     */
    public Record getRecordFromQueue(String key) {
        String queueKey = key == null ? NULL_KEY_QUEUE : key;
        KeyBasedQueue queue = queues.get(queueKey);
        if (queue == null) {
            return null;
        }
        Record record = queue.poll();
        updateQueueSizeMap(queueKey);
        return record;
    }

    /**
     * Gets the current size of the queue associated with the given key.
     *
     * @param key The key for the queue.
     * @return The size of the queue, or 0 if the queue doesn't exist.
     */
    public int getQueueSize(String key) {
        String queueKey = key == null ? NULL_KEY_QUEUE : key;
        KeyBasedQueue queue = queues.get(queueKey);
        return queue != null ? queue.size() : 0;
    }

    /**
     * !!!!!!! Must be added to metrics and values should be taken to scheduler from metrics? !!!!!
     * Updates the queue size map to reflect the current size of the queue.
     *
     * @param key The key for the queue.
     */
    private void updateQueueSizeMap(String key) {
        KeyBasedQueue queue = queues.get(key);
        if (queue == null) {
            queueSizeMap.remove(key);
        } else {
            queueSizeMap.put(key, queue.size());
        }
    }

    /**
     * Retrieves the size of the queue from the size map (cached value).
     *
     * @param key The key for the queue.
     * @return The cached size of the queue, or 0 if the queue doesn't exist in the map.
     */
    public int getCachedQueueSize(String key) {
        String queueKey = key == null ? NULL_KEY_QUEUE : key;
        return queueSizeMap.getOrDefault(queueKey, 0);
    }

    /**
     * Retrieves the total number of available records across all queues.
     *
     * @return The sum of the sizes of all queues.
     */
    public int getAvailableRecordsInQueuesSize() {
        return queueSizeMap.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Sets the observer for the QueueManager.
     *
     * @param observer The scheduler observer.
     */
    public void setSchedulerObserver(Scheduler observer) {
        this.schedulerObserver = observer;
    }

}
