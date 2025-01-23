package com.thulawa.kafka.scheduler;

import com.thulawa.kafka.ThulawaTask;
import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.HIGH_PRIORITY_THREAD_POOL;
import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.LOW_PRIORITY_THREAD_POOL;
import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.THULAWA_MAIN_THREAD_POOL;

public class ThulawaScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaScheduler.class);

    private static ThulawaScheduler instance;

    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaTaskManager thulawaTaskManager;
    private final Processor processor;
    private final Set<String> highPriorityKeySet;

    private final ThulawaMetrics thulawaMetrics;

    private State state;

    private ThulawaScheduler(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry, ThulawaTaskManager thulawaTaskManager,
                             ThulawaMetrics thulawaMetrics, Processor processor, Set<String> highPriorityKeySet) {
        this.queueManager = queueManager;
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaTaskManager = thulawaTaskManager;
        this.thulawaMetrics = thulawaMetrics;
        this.processor = processor;
        this.highPriorityKeySet = highPriorityKeySet;
        this.state = State.CREATED;

        this.queueManager.setSchedulerObserver(this);
    }

    public static synchronized ThulawaScheduler getInstance(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry, ThulawaTaskManager thulawaTaskManager,
                                                            ThulawaMetrics thulawaMetrics, Processor processor, Set<String> highPriorityKeySet) {
        if (instance == null) {
            instance = new ThulawaScheduler(queueManager, threadPoolRegistry, thulawaTaskManager, thulawaMetrics, processor, highPriorityKeySet);
        }
        return instance;
    }

    /**
     * 1. Listens for arriving events.
     * 2. Distributes events between HIGH_PRIORITY and LOW_PRIORITY threads.
     * 3. Updates the ThulawaTaskManager dynamically based on queue activity.
     * 4. Creates or deletes threads based on requirements.
     */
    public void schedule() {
        this.state = State.ACTIVE;
        logger.info("Scheduler is now ACTIVE");

        while (this.state == State.ACTIVE) {
            try {
                // High-priority processing
                for (String highPriorityKey : highPriorityKeySet) {
                    Record record = queueManager.getRecordFromQueue(highPriorityKey);
                    if (record != null) {
                        ThulawaTask highPriorityTask = new ThulawaTask(
                                HIGH_PRIORITY_THREAD_POOL,
                                record,
                                () -> processor.process(record)
                        );
                        thulawaTaskManager.addActiveTask(HIGH_PRIORITY_THREAD_POOL, highPriorityTask);
                    }
                }

                // Low-priority processing
                Record lowPriorityRecord = queueManager.getRecordFromQueue("low.priority.keys");
                if (lowPriorityRecord != null) {
                    ThulawaTask lowPriorityTask = new ThulawaTask(
                            LOW_PRIORITY_THREAD_POOL,
                            lowPriorityRecord,
                            () -> processor.process(lowPriorityRecord)
                    );
                    thulawaTaskManager.addActiveTask(LOW_PRIORITY_THREAD_POOL, lowPriorityTask);
                }

            } catch (Exception e) {
                logger.error("Error in scheduler: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void startSchedulingThread() {
        synchronized (this) {
            if (state == State.ACTIVE) {
                logger.warn("Scheduler thread is already running.");
                return;
            }
            this.threadPoolRegistry.getThreadPool(THULAWA_MAIN_THREAD_POOL).submit(this::schedule);
            this.state = State.ACTIVE;
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
        CREATED,
        ACTIVE,
        INACTIVE,
        DEAD
    }
}
