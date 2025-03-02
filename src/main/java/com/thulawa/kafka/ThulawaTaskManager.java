package com.thulawa.kafka;

import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThulawaTaskManager handles the management of active tasks assigned to threads.
 */
public class ThulawaTaskManager {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaTaskManager.class);

    // Map of thread names to their assigned active tasks
    private final Queue<ThulawaTask> assignedActiveTasks = new ConcurrentLinkedQueue<>();

    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaMetrics thulawaMetrics;
    private final ThulawaMetricsRecorder thulawaMetricsRecorder;
    private final Semaphore taskExecutionSemaphore = new Semaphore(100);
    private final AtomicInteger successCounter = new AtomicInteger(0);

    private final boolean microBatcherEnabled;

    private ThulawaTaskManager.State state;

    public ThulawaTaskManager(ThreadPoolRegistry threadPoolRegistry, ThulawaMetrics thulawaMetrics, ThulawaMetricsRecorder thulawaMetricsRecorder, boolean microBatcherEnabled) {
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaMetrics = thulawaMetrics;
        this.thulawaMetricsRecorder = thulawaMetricsRecorder;
        this.microBatcherEnabled = microBatcherEnabled;
        this.state = State.CREATED;
    }

    /**
     * Adds a task to the queue of active tasks for a specific thread.
     * Creates a new task queue for the thread if it doesn't already exist.
     *
     * @param threadPoolName The name of the thread.
     * @param thulawaTask    The task to add.
     */
    public void addActiveTask(String threadPoolName, ThulawaTask thulawaTask) {
//        // Ensure the thread has a task queue
//        assignedActiveTasks.computeIfAbsent(threadPoolName, k -> new ConcurrentLinkedQueue<>());

        // Add the task to the thread's task queue
        assignedActiveTasks.add(thulawaTask);

        if (state != State.ACTIVE) {
            startTaskManagerThread();
        }
    }

    /**
     * Submits ThulawaTasks from assignedActiveTasks to the appropriate thread pools.
     */
    public void microBatcherDisabledTaskSubmission() {
//        synchronized (this) {
//            // Check if there are tasks to process
//            if (assignedActiveTasks.isEmpty()) {
//                logger.info("No tasks to process.");
//                return;
//            }
//
//            while (this.state == State.ACTIVE) {
//                for (String threadPoolName : assignedActiveTasks.keySet()) {
//                    Queue<ThulawaTask> taskQueue = assignedActiveTasks.get(threadPoolName);
//
//                    ThulawaTask task = taskQueue.poll();
//                    if (task != null) {
//                        int processedRecordCount = task.getRecords().size();
//
//                        // Log metrics based on priority
//                        if (Objects.equals(task.getPriority(), "high-priority")) {
//                            thulawaMetricsRecorder.updateHighPriorityTasks(processedRecordCount);
//                        } else {
//                            thulawaMetricsRecorder.updateLowPriorityTasks(processedRecordCount);
//                        }
//
//                        // Submit the task to the appropriate thread pool
//                        threadPoolRegistry.getThreadPool(threadPoolName).submit(task.getRunnableProcess());
//                    }
//                }
//            }
//        }
        while (this.state == State.ACTIVE) {
            if (!assignedActiveTasks.isEmpty() && taskExecutionSemaphore.tryAcquire()) {
                ThulawaTask task = assignedActiveTasks.poll();
                if (task == null) {
                    taskExecutionSemaphore.release();
                    return;
                }

                CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
                            try {
                                task.getRunnableProcess().run();
                                return null;
                            } catch (Exception e) {
                                logger.error("Task failed: {}", e.getMessage());
                                throw new CompletionException(e);
                            }
                        }, threadPoolRegistry.getThreadPool(ThreadPoolRegistry.THULAWA_EXECUTOR_THREAD_POOL))
                        .whenComplete((r, t) -> {
                            taskExecutionSemaphore.release();
                            if (t == null) {
                                successCounter.incrementAndGet();
                                logger.info("Task completed successfully. Total success count: {}", successCounter.get());
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

    public void microBatcherEnabledTaskSubmission() {

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
            if (this.microBatcherEnabled) {
                this.threadPoolRegistry.getThreadPool(ThreadPoolRegistry.THULAWA_TASK_MANAGER_THREAD_POOL).submit(this::microBatcherEnabledTaskSubmission);
            } else {
                this.threadPoolRegistry.getThreadPool(ThreadPoolRegistry.THULAWA_TASK_MANAGER_THREAD_POOL).submit(this::microBatcherDisabledTaskSubmission);
            }
            this.state = State.ACTIVE;
        }
    }

    private enum State {
        CREATED,
        ACTIVE
    }

}
