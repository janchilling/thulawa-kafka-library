package com.thulawa.kafka.scheduler;

import com.thulawa.kafka.ThulawaTask;
import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.HIGH_PRIORITY_THREAD_POOL;
import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.THULAWA_MAIN_THREAD_POOL;


public class ThulawaScheduler implements Scheduler{

    private static final Logger logger = LoggerFactory.getLogger(ThulawaScheduler.class);

    private static ThulawaScheduler instance;

    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaTaskManager thulawaTaskManager;
    private final Processor processor;

    private State state;

    private ThulawaScheduler(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry, ThulawaTaskManager thulawaTaskManager, Processor processor) {
        this.queueManager = queueManager;
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaTaskManager = thulawaTaskManager;
        this.processor = processor;
        this.state = State.CREATED;

        this.queueManager.setSchedulerObserver(this);
    }

    public static synchronized ThulawaScheduler getInstance(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry, ThulawaTaskManager thulawaTaskManager, Processor processor) {
        if (instance == null) {
            instance = new ThulawaScheduler(queueManager, threadPoolRegistry, thulawaTaskManager, processor);
        }
        return instance;
    }

    /**
     *  1. Listens for arriving events .
     *  2. Listen for JVM Metrics.
     *  3. Update the ThulawaTaskManager with the necessary tasks.
     *  eg - Dynamically manage the Runnable Task slots based on thread busyness.
     *  4. Create or Delete thread based on the requirements.
     *
     */
    public void schedule() {
        this.state = State.ACTIVE;
        logger.info("Scheduler is now ACTIVE");

        while (this.state == State.ACTIVE) {
            try {
                int availableRecordsInQueues = queueManager.getAvailableRecordsInQueuesSize();

                if (availableRecordsInQueues > 0) {
                    ThulawaTask thulawaTask = new ThulawaTask(HIGH_PRIORITY_THREAD_POOL,
                            queueManager.getRecordFromQueue("__NULL_KEY__"),
                            () -> processor.process(queueManager.getRecordFromQueue("__NULL_KEY__")));
                    this.thulawaTaskManager.addActiveTask(HIGH_PRIORITY_THREAD_POOL, thulawaTask);
                }
            } catch (Exception e) {
                logger.error("Error in scheduler: {}", e.getMessage());
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

    private enum State{
        CREATED,
        ACTIVE,
        INACTIVE,
        DEAD
    }

}
