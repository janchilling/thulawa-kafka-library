package com.thulawa.kafka.internals.processor;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.ThulawaEvent;
import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.configs.ThulawaConfigs;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.JVMMetricsRecorder;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import com.thulawa.kafka.scheduler.ThulawaScheduler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.thulawa.kafka.ThulawaKafkaStreams.THULAWA_METRICS_CONFIG;
import static com.thulawa.kafka.internals.configs.ThulawaConfigs.HIGH_PRIORITY_KEY_MAP;

/**
 * ThulawaProcessor processes Kafka records and manages key-based queues dynamically.
 */
public class ThulawaProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaProcessor.class);

    private QueueManager queueManager;
    private ThulawaScheduler thulawaScheduler;
    private ThulawaTaskManager thulawaTaskManager;
    private ThulawaMetrics thulawaMetrics;
    private MicroBatcher microBatcher;

    private Processor processor;
    private ProcessorContext processorContext;

    private JVMMetricsRecorder jvmMetricsRecorder;
    private ThulawaMetricsRecorder thulawaMetricsRecorder;

    public ThulawaProcessor(Processor processor) {
        this.processor = processor;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {

        this.processorContext = context;

        this.thulawaMetrics = (ThulawaMetrics) context.appConfigs().get(THULAWA_METRICS_CONFIG);

        initializeRecoders(this.thulawaMetrics);
        this.queueManager = QueueManager.getInstance((Set<String>) context.appConfigs().get(HIGH_PRIORITY_KEY_MAP));
        this.microBatcher = new MicroBatcher(this.queueManager);
        ThreadPoolRegistry threadPoolRegistry = ThreadPoolRegistry.getInstance((Integer) context.appConfigs().get(ThulawaConfigs.THULAWA_EXECUTOR_THREADPOOL_SIZE));
        this.thulawaTaskManager = new ThulawaTaskManager(threadPoolRegistry, this.thulawaMetrics, this.microBatcher, this.thulawaMetricsRecorder, (Boolean) context.appConfigs().get(ThulawaConfigs.PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED));
        this.thulawaScheduler = ThulawaScheduler.getInstance(this.queueManager, threadPoolRegistry,
                this.thulawaTaskManager, thulawaMetrics, processor, (Set<String>) context.appConfigs().get(HIGH_PRIORITY_KEY_MAP),
                (Boolean) context.appConfigs().get(ThulawaConfigs.PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED)
        );

    }

    private void initializeRecoders(ThulawaMetrics thulawaMetrics) {
        this.jvmMetricsRecorder = new JVMMetricsRecorder(thulawaMetrics);
        this.thulawaMetricsRecorder = new ThulawaMetricsRecorder(thulawaMetrics);
    }

    @Override
    public void process(Record<KIn, VIn> record) {

        String key = (String) record.key();
        long receivedSystemTIme = processorContext.currentSystemTimeMs();
        Runnable runnableProcess = () ->{
            this.processor.process(record);
        };

        ThulawaEvent thulawaEvent = new ThulawaEvent(record, receivedSystemTIme, runnableProcess);

        queueManager.addToKeyBasedQueue(key, thulawaEvent);
    }


    @Override
    public void close() {
        // Shut down the thread pool
        logger.info("Processor closed. Processed records count: {}");
    }
}
