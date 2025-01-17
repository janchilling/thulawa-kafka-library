package com.thulawa.kafka.internals.processor;

import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.scheduler.ThulawaScheduler;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thulawa.kafka.ThulawaKafkaStreams.THULAWA_METRICS_CONFIG;

/**
 * ThulawaProcessor processes Kafka records and manages key-based queues dynamically.
 */
public class ThulawaProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaProcessor.class);

    private QueueManager queueManager;
    private ThreadPoolRegistry threadPoolRegistry;
    private ThulawaScheduler thulawaScheduler;
    private ThulawaTaskManager thulawaTaskManager;
    private ThulawaMetrics thulawaMetrics;

    private Processor processor;
    private ProcessorContext processorContext;

    public ThulawaProcessor(Processor processor) {
        this.processor = processor;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {

        this.processorContext = context;

        this.thulawaMetrics = (ThulawaMetrics) context.appConfigs().get(THULAWA_METRICS_CONFIG);

        this.queueManager = QueueManager.getInstance();
        this.threadPoolRegistry = ThreadPoolRegistry.getInstance();
        this.thulawaTaskManager = new ThulawaTaskManager(this.threadPoolRegistry);
        this.thulawaScheduler = ThulawaScheduler.getInstance(this.queueManager, this.threadPoolRegistry,
                this.thulawaTaskManager, thulawaMetrics, processor);

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
