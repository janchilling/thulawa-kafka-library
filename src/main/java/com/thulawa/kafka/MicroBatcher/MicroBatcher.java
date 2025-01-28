package com.thulawa.kafka.MicroBatcher;

import com.thulawa.kafka.internals.helpers.QueueManager;
import org.apache.kafka.streams.processor.api.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MicroBatcher {

    private final QueueManager queueManager;

    public MicroBatcher(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    /**
     * Retrieves a batch of records from the queue for the given key.
     *
     * @param key       The key for the queue to fetch records.
     * @param batchSize The number of records to fetch in a batch.
     * @return A list of records.
     */
    public List<Record> fetchBatch(String key, int batchSize) {
        List<Record> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Record record = queueManager.getRecordFromQueue(key);
            if (record != null) {
                batch.add(record);
            } else {
                break; // Stop fetching if no more records are available
            }
        }
        return batch;
    }
}
