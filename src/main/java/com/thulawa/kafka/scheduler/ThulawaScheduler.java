package com.thulawa.kafka.scheduler;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.ThulawaEvent;
import com.thulawa.kafka.ThulawaTask;
import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ThulawaScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaScheduler.class);
    private static final int BATCH_SIZE = 3;

    private static ThulawaScheduler instance;
    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaTaskManager thulawaTaskManager;
    private final Set<String> highPriorityKeySet;
    private final ThulawaMetrics thulawaMetrics;
    private final MicroBatcher microbatcher;
    private State state;

    private ThulawaScheduler(QueueManager queueManager,
                             ThreadPoolRegistry threadPoolRegistry,
                             ThulawaTaskManager thulawaTaskManager,
                             ThulawaMetrics thulawaMetrics,
                             Set<String> highPriorityKeySet) {
        this.queueManager = queueManager;
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaTaskManager = thulawaTaskManager;
        this.thulawaMetrics = thulawaMetrics;
        this.highPriorityKeySet = highPriorityKeySet;
        this.state = State.CREATED;
        this.microbatcher = new MicroBatcher(queueManager);
        this.queueManager.setSchedulerObserver(this);
    }

    public static synchronized ThulawaScheduler getInstance(QueueManager queueManager,
                                                            ThreadPoolRegistry threadPoolRegistry,
                                                            ThulawaTaskManager thulawaTaskManager,
                                                            ThulawaMetrics thulawaMetrics,
                                                            Set<String> highPriorityKeySet) {
        if (instance == null) {
            instance = new ThulawaScheduler(
                    queueManager,
                    threadPoolRegistry,
                    thulawaTaskManager,
                    thulawaMetrics,
                    highPriorityKeySet);
        }
        return instance;
    }

    public void runScheduler() {
        this.state = State.ACTIVE;
        logger.info("Scheduler is now ACTIVE");

        while (this.state == State.ACTIVE) {
            try {
                processBatch();
            } catch (Exception e) {
                logger.error("Error in scheduler: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processBatch() {
        Object headQueueKey = queueManager.getEarliestQueueKey();
        if(headQueueKey != null){
            List<ThulawaEvent> batch = microbatcher.fetchAdaptiveBatch(headQueueKey);
            if (!batch.isEmpty()) {
                ThulawaTask task = new ThulawaTask(ThreadPoolRegistry.THULAWA_EXECUTOR_THREAD_POOL,
                        batch);
                thulawaTaskManager.addActiveTask(headQueueKey, task);
            }
        }
    }

    public void startSchedulingThread() {
        synchronized (this) {
            if (state == State.ACTIVE) {
                logger.warn("Scheduler thread is already running.");
                return;
            }
            logger.info("Starting the Task Manager dedicated thread.");

            threadPoolRegistry.startDedicatedThread("Thulawa-Scheduler-Thread", this::runScheduler);

            this.state = ThulawaScheduler.State.ACTIVE;
        }
    }

    @Override
    public void notifyScheduler() {
        logger.info("Scheduler notified by QueueManager.");
        if (state != State.ACTIVE) {
            startSchedulingThread();
        }
    }

    @Override
    public boolean isActive() {
        return this.state == State.ACTIVE;
    }

    private enum State {
        CREATED, ACTIVE
    }
}
