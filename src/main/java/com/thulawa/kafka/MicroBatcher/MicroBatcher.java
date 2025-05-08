package com.thulawa.kafka.MicroBatcher;

import com.thulawa.kafka.ThulawaEvent;
import com.thulawa.kafka.internals.helpers.QueueManager;
import org.apache.kafka.streams.processor.api.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MicroBatcher {

    private final QueueManager queueManager;

    private final Map<Object, Double> processingLatencyAvg = new HashMap<>();
    private final Map<Object, Double> eventsPerTaskAvg = new HashMap<>();
    private final Map<Object, Long> taskCount = new HashMap<>();
    private final Map<Object, Double> batchSizeEWMA = new ConcurrentHashMap<>();

    private static final double LATENCY_THRESHOLD = 500.0;
    private static final double EWMA_ALPHA = 0.2;
    private static final int MIN_BATCH_SIZE = 1;
    private static final int MAX_BATCH_SIZE = 100;

    public MicroBatcher(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    /**
     * Retrieves a batch of records from the queue for the given key.
     *
     * @param headQueueKey The key to fetch records for.
     * @param batchSize    The number of records to fetch in a batch.
     * @return A list of ThulawaEvent records.
     */
    public List<ThulawaEvent> fetchBatch(Object headQueueKey, int batchSize) {
        return queueManager.getRecordBatchesFromKBQueues(headQueueKey, batchSize);
    }

    /**
     * Adaptive batch size calculation based on processing latency, average events per task, and queue size.
     *
     * @param key The key for which a batch needs to be fetched.
     * @return A batch of events.
     */
    public synchronized List<ThulawaEvent> fetchAdaptiveBatch(Object key) {
        int queueSize = queueManager.sizeOfKeyBasedQueue(key);
        if (queueSize == 0) {
            return List.of();
        }

        double avgLatency = processingLatencyAvg.getOrDefault(key, LATENCY_THRESHOLD);
        double avgEventsPerTask = eventsPerTaskAvg.getOrDefault(key, 10.0);

        double latencyFactor = LATENCY_THRESHOLD / (avgLatency + 1);
        double adjustedBatchSize = avgEventsPerTask * latencyFactor;

        int proposedBatchSize = (int) Math.min(Math.max(adjustedBatchSize, MIN_BATCH_SIZE), queueSize);

        double previousBatchSize = batchSizeEWMA.getOrDefault(key, (double) proposedBatchSize);
        double smoothedBatchSize = EWMA_ALPHA * proposedBatchSize + (1 - EWMA_ALPHA) * previousBatchSize;

        int finalBatchSize = (int) Math.min(Math.max(smoothedBatchSize, MIN_BATCH_SIZE), MAX_BATCH_SIZE);
        batchSizeEWMA.put(key, (double) finalBatchSize);

        return queueManager.getRecordBatchesFromKBQueues(key, finalBatchSize);
    }

    /**
     * Updates the running average of processing latency per key.
     *
     * @param key            The key associated with the task.
     * @param processingTime The processing time of the completed task.
     */
    public void updateProcessingLatency(Object key, long processingTime, int totalEventsInTask) {
        taskCount.putIfAbsent(key, 0L);
        processingLatencyAvg.putIfAbsent(key, 0.0);
        eventsPerTaskAvg.putIfAbsent(key, 0.0);

        taskCount.put(key, taskCount.get(key) + 1);
        long count = taskCount.get(key);

        double prevProcessingLatencyAvg = processingLatencyAvg.get(key);
        double prevEventPerTaskAvg = eventsPerTaskAvg.get(key);

        double newProcessingLatencyAvg = prevProcessingLatencyAvg + (processingTime - prevProcessingLatencyAvg) / count;
        processingLatencyAvg.put(key, newProcessingLatencyAvg);

        double newEventPerTaskAvg = prevEventPerTaskAvg + (totalEventsInTask - prevEventPerTaskAvg) / count;
        eventsPerTaskAvg.put(key, newEventPerTaskAvg);
    }


    /**
     * Retrieves the latest average processing latency for a given key.
     *
     * @param key The key to fetch the latency for.
     * @return The average latency, or 0.0 if no data exists.
     */
    public double getProcessingLatencyAvg(Object key) {
        return processingLatencyAvg.getOrDefault(key, 0.0);
    }

    /**
     * Retrieves the latest average events per task for a given key.
     *
     * @param key The key to fetch the latency for.
     * @return The average latency, or 0.0 if no data exists.
     */
    public double getEventPerTaskAvg(Object key) {
        return processingLatencyAvg.getOrDefault(key, 0.0);
    }
}
