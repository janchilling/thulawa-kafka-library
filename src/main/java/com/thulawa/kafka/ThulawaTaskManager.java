package com.thulawa.kafka;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * ThulawaTaskManager handles the management of active tasks assigned to threads.
 */
public class ThulawaTaskManager {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaTaskManager.class);

    private final Map<String, Queue<ThulawaTask>> assignedActiveTasks = new ConcurrentHashMap<>();
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaMetrics thulawaMetrics;
    private final MicroBatcher microBatcher;
    private final ThulawaMetricsRecorder thulawaMetricsRecorder;
    private final Semaphore taskExecutionSemaphore = new Semaphore(100);
    private final AtomicLong totalSuccessCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, LongAdder> keyBasesSuccessCounter = new ConcurrentHashMap<>();
    private final boolean microBatcherEnabled;

    private final Map<String, KeyProcessingState> keySetState = new ConcurrentHashMap<>();
    private State state;

    public ThulawaTaskManager(ThreadPoolRegistry threadPoolRegistry, ThulawaMetrics thulawaMetrics, MicroBatcher microBatcher,
                              ThulawaMetricsRecorder thulawaMetricsRecorder, boolean microBatcherEnabled) {
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaMetrics = thulawaMetrics;
        this.microBatcher = microBatcher;
        this.thulawaMetricsRecorder = thulawaMetricsRecorder;
        this.microBatcherEnabled = microBatcherEnabled;
        this.state = State.CREATED;
    }

    /**
     * Adds a task to the queue of active tasks for a specific key.
     * Creates a new task queue for the key if it doesn't already exist.
     *
     * @param key         The key of the task.
     * @param thulawaTask The task to add.
     */
    public void addActiveTask(String key, ThulawaTask thulawaTask) {
        assignedActiveTasks.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(thulawaTask);

        // Ensure key is marked as NOT_PROCESSING if it's a new key
        keySetState.putIfAbsent(key, KeyProcessingState.NOT_PROCESSING);

        if (state != State.ACTIVE) {
            startTaskManagerThread();
        }
    }

    /**
     * Submits tasks from assignedActiveTasks to the appropriate thread pools.
     */
    public void submitTasksForProcessing() {
        while (this.state == State.ACTIVE) {
            for (String key : assignedActiveTasks.keySet()) {
                Queue<ThulawaTask> taskQueue = assignedActiveTasks.get(key);

                if (taskQueue == null || taskQueue.isEmpty() || keySetState.get(key) == KeyProcessingState.PROCESSING) {
                    continue;
                }

                keySetState.put(key, KeyProcessingState.PROCESSING);
                ThulawaTask task = taskQueue.poll();
                if (task == null) {
                    keySetState.put(key, KeyProcessingState.NOT_PROCESSING);
                    continue;
                }

                long startTime = System.nanoTime();
                int totalEventsInTask = task.getThulawaEvents().size();

                if (!taskExecutionSemaphore.tryAcquire()) {
                    keySetState.put(key, KeyProcessingState.NOT_PROCESSING);
                    continue;
                }

                CompletableFuture.runAsync(() -> {
                            try {
                                task.getThulawaEvents().forEach(thulawaEvent -> thulawaEvent.getRunnableProcess().run());
                            } catch (Exception e) {
                                logger.error("Task failed: {}", e.getMessage());
                                throw new CompletionException(e);
                            }
                        }, Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()))
                        .whenComplete((r, t) -> {
                            long endTime = System.nanoTime();
                            long processingTime = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                            microBatcher.updateProcessingLatency(key, processingTime, totalEventsInTask);

                            taskExecutionSemaphore.release();
                            keySetState.put(key, KeyProcessingState.NOT_PROCESSING);

                            if (t == null) {
                                incrementSuccessCount(key, totalEventsInTask);
                                double avgLatency = microBatcher.getProcessingLatencyAvg(key);
//                                logger.info("Updated processing latency average for {}: {} ms", key, avgLatency);
                            } else {
                                handleFailure(task, t);
                            }
                        })
                        .exceptionally(ex -> {
                            handleFatalException(task, ex);
                            throw new RuntimeException(ex);
                        });
            }
        }
    }

    private void handleFatalException(ThulawaTask task, Throwable fatalException) {
        logger.error("Fatal exception in task: {}", fatalException.getMessage());
    }

    private void handleFailure(ThulawaTask task, Throwable exception) {
        logger.warn("Task failed and will not be retried: {}", exception.getMessage());
    }

    /**
     * Starts the task manager thread and switches its state to ACTIVE.
     */
    public void startTaskManagerThread() {
        synchronized (this) {
            if (state == State.ACTIVE) {
                logger.warn("Task Manager thread is already running.");
                return;
            }

            logger.info("Starting the Task Manager thread.");

            this.threadPoolRegistry
                    .getThreadPool(ThreadPoolRegistry.THULAWA_TASK_MANAGER_THREAD_POOL)
                    .submit(this::submitTasksForProcessing);

            this.state = State.ACTIVE;
        }
    }

    public void incrementSuccessCount(String key, int totalEventsInTask) {
        keyBasesSuccessCounter.computeIfAbsent(key, k -> new LongAdder()).add(totalEventsInTask);
        totalSuccessCount.addAndGet(totalEventsInTask);
        thulawaMetricsRecorder.updateTotalProcessedCount(getTotalSuccessCount());
    }

    public long getSuccessCount(String key) {
        return keyBasesSuccessCounter.getOrDefault(key, new LongAdder()).sum();
    }

    public long getTotalSuccessCount() {
        return totalSuccessCount.get();
    }


    private enum State {
        CREATED,
        ACTIVE
    }

    private enum KeyProcessingState {
        PROCESSING,
        NOT_PROCESSING
    }
}
