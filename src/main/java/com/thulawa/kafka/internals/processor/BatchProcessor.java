package com.thulawa.kafka.internals.processor;

import com.thulawa.kafka.internals.helpers.BatchWorkerThreadPool;
import com.thulawa.kafka.internals.storage.KeyBasedQueue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final Map<String, KeyBasedQueue> queues = new ConcurrentHashMap<>();
    private final AtomicInteger nullQueueCounter = new AtomicInteger(0);
    private final AtomicInteger counter = new AtomicInteger(0);

    private final BatchWorkerThreadPool threadPool;

    public BatchProcessor() {
        // Initialize the thread pool with 4 workers and a task queue of size 100
        threadPool = new BatchWorkerThreadPool(4, 100);
    }

    @Override
    public void process(Record<KIn, VIn> record) {





    }

    private synchronized void processQueues() {
        queues.values().forEach(q -> {
            while (!q.isEmpty()) {
                logger.info("Processed Value: {}", q.poll());
            }
        });
        counter.set(0); // Reset counter
    }

    @Override
    public void close() {
        // Shut down the thread pool
        threadPool.shutdown();
    }
}
