package com.thulawa.kafka.internals.processor;

import com.thulawa.kafka.internals.storage.Queue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private final Map<String, Queue> queues = new ConcurrentHashMap<>(); // Stores queues based on unique names
    private final AtomicInteger nullQueueCounter = new AtomicInteger(0); // Counter for null key queues

    @Override
    public void process(Record<KIn, VIn> record) {

        String queueName;
        if (record.key() == null) {
            queueName = "null_queue_" + nullQueueCounter.incrementAndGet();
        } else {
            queueName = "queue_" + record.key().toString();
        }

        // Retrieve or create the queue
        Queue queue = queues.computeIfAbsent(queueName, k -> new Queue());

        // Add the record to the queue
        queue.add(record);

        // (Optional) Keep track of record counts or perform other logic here

    }
}
