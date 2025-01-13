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

    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaScheduler thulawaScheduler;
    private final ThulawaTaskManager thulawaTaskManager;

    public ThulawaProcessor(Processor processor) {
        this.queueManager = QueueManager.getInstance();
        this.threadPoolRegistry = ThreadPoolRegistry.getInstance();
        this.thulawaTaskManager = new ThulawaTaskManager(this.threadPoolRegistry);
        this.thulawaScheduler = ThulawaScheduler.getInstance(this.queueManager, this.threadPoolRegistry, this.thulawaTaskManager, processor);

    }

    @Override
    public void process(Record<KIn, VIn> record) {
        // Extract the key from the record
        String key = (String) record.key();

        // Add the record to the queue
        queueManager.addToKeyBasedQueue(key, record);
    }


    @Override
    public void close() {
        // Shut down the thread pool
        logger.info("Processor closed. Processed records count: {}");
    }
}
