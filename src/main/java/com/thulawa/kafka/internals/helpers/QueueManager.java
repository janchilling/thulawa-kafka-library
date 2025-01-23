package com.thulawa.kafka.internals.helpers;

import com.thulawa.kafka.internals.storage.KeyBasedQueue;
import com.thulawa.kafka.scheduler.Scheduler;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueueManager dynamically manages key-based queues for storing records.
 */
public class QueueManager {

    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);

    private static QueueManager instance;

    private static final String COMMON_QUEUE_KEY = "low.priority.keys";

    // Map to hold queues for each key
    private final Map<String, KeyBasedQueue> queues = new ConcurrentHashMap<>();

    // Map to track the size of each queue
    private final Map<String, Integer> queueSizeMap = new ConcurrentHashMap<>();

    private final Set<String> highPriorityKeySet;

    private Scheduler schedulerObserver;

    private QueueManager(Set<String> highPriorityKeySet) {
        this.highPriorityKeySet = highPriorityKeySet;
    }

    public static synchronized QueueManager getInstance(Set<String> highPriorityKeySet) {
        if (instance == null) {
            instance = new QueueManager(highPriorityKeySet);
        }
        return instance;
    }

    /**
     * Adds a record to the appropriate queue based on the key.
     * For high-priority keys, a dedicated queue is created.
     * For all other keys, records are added to a common queue: "low.priority.keys".
     *
     * @param key    The key for the queue (nullable).
     * @param record The record to add.
     */
    public void addToKeyBasedQueue(String key, Record record) {
        String queueKey;

        // Determine the appropriate queue based on whether the key is high priority
        if (key != null && highPriorityKeySet.contains(key)) {
            queueKey = key; // High-priority keys get their own queue
            queues.computeIfAbsent(queueKey, k -> new KeyBasedQueue(key));
        } else {
            queueKey = COMMON_QUEUE_KEY; // Low-priority keys go to a common queue
            queues.computeIfAbsent(queueKey, k -> new KeyBasedQueue(COMMON_QUEUE_KEY));
        }

        // Add the record to the determined queue
        queues.get(queueKey).add(record);
        updateQueueSizeMap(queueKey);

        // Notify observer if scheduler is not ACTIVE
        if (schedulerObserver != null && !schedulerObserver.isActive()) {
            schedulerObserver.notifyScheduler();
        }
    }

    /**
     * Retrieves and removes a record from the queue associated with the given key.
     * For non-high-priority keys, retrieves records from the common queue: "low.priority.keys".
     *
     * @param key The key for the queue (nullable).
     * @return The retrieved record, or null if the queue is empty or doesn't exist.
     */
    public Record getRecordFromQueue(String key) {
        String queueKey;

        // Determine the appropriate queue to retrieve from
        if (key != null && highPriorityKeySet.contains(key)) {
            queueKey = key;
        } else {
            queueKey = COMMON_QUEUE_KEY;
        }

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
     * For non-high-priority keys, gets the size of the common queue: "low.priority.keys".
     *
     * @param key The key for the queue (nullable).
     * @return The size of the queue, or 0 if the queue doesn't exist.
     */
    public int getQueueSize(String key) {
        String queueKey;

        // Determine the appropriate queue to check size
        if (key != null && highPriorityKeySet.contains(key)) {
            queueKey = key;
        } else {
            queueKey = COMMON_QUEUE_KEY;
        }

        KeyBasedQueue queue = queues.get(queueKey);
        return queue != null ? queue.size() : 0;
    }

    /**
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
     * For non-high-priority keys, retrieves the cached size of the common queue: "low.priority.keys".
     *
     * @param key The key for the queue (nullable).
     * @return The cached size of the queue, or 0 if the queue doesn't exist in the map.
     */
    public int getCachedQueueSize(String key) {
        String queueKey;

        // Determine the appropriate queue to check cached size
        if (key != null && highPriorityKeySet.contains(key)) {
            queueKey = key;
        } else {
            queueKey = COMMON_QUEUE_KEY;
        }

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
