package com.thulawa.kafka.internals.helpers;

import com.thulawa.kafka.ThulawaEvent;
import com.thulawa.kafka.internals.storage.KeyBasedQueue;
import com.thulawa.kafka.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * QueueManager dynamically manages key-based queues for storing records.
 */
public class QueueManager {

    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    private static QueueManager instance;
    private static final String COMMON_QUEUE_KEY = "low.priority.keys";

    private final Map<Object, KeyBasedQueue> queues = new ConcurrentHashMap<>();
    private final Map<Object, Long> queueEarliestTimestamps = new ConcurrentHashMap<>();
    private final Set<Object> highPriorityKeySet;
    private Scheduler schedulerObserver;
    private final ReentrantLock lock = new ReentrantLock();

    private ThulawaEvent headEvent;
    private Object headQueueKey;

    private QueueManager(Set<Object> highPriorityKeySet) {
        this.highPriorityKeySet = highPriorityKeySet;
    }

    public static synchronized QueueManager getInstance(Set<Object> highPriorityKeySet) {
        if (instance == null) {
            instance = new QueueManager(highPriorityKeySet);
        }
        return instance;
    }

    public void addToKeyBasedQueue(Object key, ThulawaEvent thulawaEvent) {
        Object queueKey = (key != null) ? key : COMMON_QUEUE_KEY;
        long timestamp = thulawaEvent.getReceivedSystemTime();

        lock.lock();
        try {
            queues.computeIfAbsent(queueKey, k -> new KeyBasedQueue(queueKey)).add(thulawaEvent);
            Long existingTimestamp = queueEarliestTimestamps.get(queueKey);

            if (existingTimestamp == null || timestamp < existingTimestamp) {
                queueEarliestTimestamps.put(queueKey, Long.valueOf(timestamp));
            }

            // Update head event if it's the oldest
            if (headEvent == null || timestamp < headEvent.getReceivedSystemTime()) {
                headEvent = thulawaEvent;
                headQueueKey = queueKey;
            }

//            removeInactiveQueues();

        } finally {
            lock.unlock();
        }

        if (schedulerObserver != null && !schedulerObserver.isActive()) {
            schedulerObserver.notifyScheduler();
        }
    }

    public Object getEarliestQueueKey() {
        lock.lock();
        try {
            return headQueueKey;
        } finally {
            lock.unlock();
        }
    }

    public ThulawaEvent getNextRecord() {
        lock.lock();
        try {
            if (headEvent == null) return null;

            KeyBasedQueue queue = queues.get(headQueueKey);
            if (queue == null || queue.isEmpty()) return null;

            ThulawaEvent removedEvent = queue.poll();

            if (!queue.isEmpty()) {
                long newEarliestTimestamp = queue.peekTimestamp();
                queueEarliestTimestamps.put(headQueueKey, Long.valueOf(newEarliestTimestamp));
            } else {
                queueEarliestTimestamps.remove(headQueueKey);
                queues.remove(headQueueKey);
            }

            // Recalculate the new head event
            headEvent = null;
            headQueueKey = null;
            for (Map.Entry<Object, Long> entry : queueEarliestTimestamps.entrySet()) {
                if (headEvent == null || entry.getValue() < headEvent.getReceivedSystemTime()) {
                    headQueueKey = entry.getKey();
                    headEvent = queues.get(entry.getKey()).peek();
                }
            }
            return removedEvent;
        } finally {
            lock.unlock();
        }
    }

    public List<ThulawaEvent> getRecordBatchesFromKBQueues(Object key, int batchSize) {
        lock.lock();
        try {
            Object queueKey = (key != null) ? key : COMMON_QUEUE_KEY;
            KeyBasedQueue queue = queues.get(queueKey);

            if (queue == null || queue.isEmpty()) {
                return Collections.emptyList();
            }

            List<ThulawaEvent> batch = new ArrayList<>();
            for (int i = 0; i < batchSize && !queue.isEmpty(); i++) {
                batch.add(queue.poll());
            }

            // Update queue timestamps and head event if needed
            if (!queue.isEmpty()) {
                queueEarliestTimestamps.put(queueKey, queue.peekTimestamp());
            } else {
                queueEarliestTimestamps.remove(queueKey);
                queues.remove(queueKey);
            }

            // Recalculate head event
            headEvent = null;
            headQueueKey = null;
            for (Map.Entry<Object, Long> entry : queueEarliestTimestamps.entrySet()) {
                if (headEvent == null || entry.getValue() < headEvent.getReceivedSystemTime()) {
                    headQueueKey = entry.getKey();
                    headEvent = queues.get(entry.getKey()).peek();
                }
            }

            return batch;
        } finally {
            lock.unlock();
        }
    }

    public int sizeOfKeyBasedQueue(Object key){
        return queues.get(key).size();
    }


    public void setSchedulerObserver(Scheduler observer) {
        this.schedulerObserver = observer;
    }

    private void removeInactiveQueues() {
        for (Object key : new HashSet<>(queues.keySet())) {
            KeyBasedQueue queue = queues.get(key);
            if (queue != null && queue.isEmpty()) {
                queues.remove(key);
                queueEarliestTimestamps.remove(key);
            }
        }
    }


}
