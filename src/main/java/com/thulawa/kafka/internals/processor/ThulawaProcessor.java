package com.thulawa.kafka.internals.processor;

import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.scheduler.ThulawaScheduler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThulawaProcessor processes Kafka records and manages key-based queues dynamically.
 */
public class ThulawaProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaProcessor.class);

    private static final String THULAWA_MAIN_POOL = "ThulawaMainPool";
    private static final String HIGH_PRIORITY_POOL = "HighPriorityPool";
    private static final String LOW_PRIORITY_POOL = "LowPriorityPool";

    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaScheduler thulawaScheduler;
    private final ThulawaTaskManager thulawaTaskManager;

    public ThulawaProcessor(Processor processor) {
        this.queueManager = new QueueManager();
        this.threadPoolRegistry = new ThreadPoolRegistry();

        // Register thread pools if not already registered
        threadPoolRegistry.registerThreadPool(THULAWA_MAIN_POOL, 1, 2);
        threadPoolRegistry.registerThreadPool(HIGH_PRIORITY_POOL, 1, 2);
        threadPoolRegistry.registerThreadPool(LOW_PRIORITY_POOL, 1, 2);
        this.thulawaTaskManager = new ThulawaTaskManager(this.threadPoolRegistry);
        this.thulawaScheduler = new ThulawaScheduler(this.queueManager, this.threadPoolRegistry, this.thulawaTaskManager, processor);

    }

    @Override
    public void process(Record<KIn, VIn> record) {
        // Extract the key from the record
        String key = (String) record.key();

        // Add the record to the queue
        queueManager.addToKeyBasedQueue(key, record);

        // Submit the scheduler logic to the main thread pool
        this.threadPoolRegistry.getThreadPool(THULAWA_MAIN_POOL).submit(() -> thulawaScheduler.schedule());
    }


    @Override
    public void close() {
        // Shut down the thread pool
        logger.info("Processor closed. Processed records count: {}");
    }
}
