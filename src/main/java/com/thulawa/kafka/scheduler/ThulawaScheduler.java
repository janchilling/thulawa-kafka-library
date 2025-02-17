package com.thulawa.kafka.scheduler;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThulawaScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaScheduler.class);
    private static final int BATCH_SIZE = 5;
    private static final int MAX_THREAD_LOAD = 50; // Threshold for adding new threads
    private static final int MAX_THREADS = 10; // Maximum allowed threads

    private static ThulawaScheduler instance;
    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaTaskManager thulawaTaskManager;
    private final Processor processor;
    private final Set<String> highPriorityKeySet;
    private final ThulawaMetrics thulawaMetrics;
    private final MicroBatcher microbatcher;
    private State state;

    private ThulawaScheduler(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry,
                             ThulawaTaskManager thulawaTaskManager, ThulawaMetrics thulawaMetrics,
                             Processor processor, Set<String> highPriorityKeySet) {
        this.queueManager = queueManager;
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaTaskManager = thulawaTaskManager;
        this.thulawaMetrics = thulawaMetrics;
        this.processor = processor;
        this.highPriorityKeySet = highPriorityKeySet;
        this.state = State.CREATED;
        this.microbatcher = new MicroBatcher(queueManager);
        this.queueManager.setSchedulerObserver(this);
    }

    public static synchronized ThulawaScheduler getInstance(QueueManager queueManager,
                                                            ThreadPoolRegistry threadPoolRegistry,
                                                            ThulawaTaskManager thulawaTaskManager,
                                                            ThulawaMetrics thulawaMetrics,
                                                            Processor processor,
                                                            Set<String> highPriorityKeySet) {
        if (instance == null) {
            instance = new ThulawaScheduler(queueManager, threadPoolRegistry, thulawaTaskManager, thulawaMetrics, processor, highPriorityKeySet);
        }
        return instance;
    }

    public void schedule() {
        this.state = State.ACTIVE;
        logger.info("Scheduler is now ACTIVE");

        while (this.state == State.ACTIVE) {
            try {
                balanceAndProcessTasks();
//                adjustThreadPool();
            } catch (Exception e) {
                logger.error("Error in scheduler: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void balanceAndProcessTasks() {
        int highPriorityWeight = 2; // Give more weight to high-priority tasks
        int lowPriorityWeight = 1;

        for (String highPriorityKey : highPriorityKeySet) {
            processBatch(highPriorityKey, "high-priority", highPriorityWeight);
        }

        processBatch("low.priority.keys", "low-priority", lowPriorityWeight);
    }

    private void processBatch(String key, String priority, int weight) {
        for (int i = 0; i < weight; i++) { // Weighted processing
            List<Record> batch = microbatcher.fetchBatch(key, BATCH_SIZE);
            if (!batch.isEmpty()) {
                ThulawaTask task = new ThulawaTask(ThreadPoolRegistry.THULAWA_EXECUTOR_THREAD_POOL,
                        batch, priority, () -> batch.forEach(processor::process));
                thulawaTaskManager.addActiveTask(ThreadPoolRegistry.THULAWA_EXECUTOR_THREAD_POOL, task);
            }
        }
    }

    private void adjustThreadPool() {
        ThreadPoolExecutor executorPool = threadPoolRegistry.getThreadPool(ThreadPoolRegistry.THULAWA_EXECUTOR_THREAD_POOL);
        int queueSize = executorPool.getQueue().size();
        int activeThreads = executorPool.getActiveCount();
        int corePoolSize = executorPool.getCorePoolSize();

        if (queueSize > MAX_THREAD_LOAD && corePoolSize < MAX_THREADS) {
            int newPoolSize = Math.min(corePoolSize + 1, MAX_THREADS);
            executorPool.setCorePoolSize(newPoolSize);
            executorPool.setMaximumPoolSize(newPoolSize);
            logger.info("Increased thread pool size to {} due to high load.", newPoolSize);
        }
    }

    public void startSchedulingThread() {
        synchronized (this) {
            if (state == State.ACTIVE) {
                logger.warn("Scheduler thread is already running.");
                return;
            }
            this.threadPoolRegistry.getThreadPool(ThreadPoolRegistry.THULAWA_SCHEDULING_THREAD_POOL).submit(this::schedule);
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
        CREATED, ACTIVE, INACTIVE, DEAD
    }
}
