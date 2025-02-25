package com.thulawa.kafka;

import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;
import java.nio.charset.StandardCharsets;

import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.thulawa.kafka.internals.helpers.ThreadPoolRegistry.THULAWA_TASK_MANAGER_THREAD_POOL;

/**
 * ThulawaTaskManager handles the management of active tasks assigned to threads.
 */
public class ThulawaTaskManager {

    private static final Logger logger = LoggerFactory.getLogger(ThulawaTaskManager.class);

    // Map of thread names to their assigned active tasks
    private final Map<String, Queue<ThulawaTask>> assignedActiveTasks = new ConcurrentHashMap<>();
    private static RocksDB db;

    private final ThreadPoolRegistry threadPoolRegistry;

    private final ThulawaMetrics thulawaMetrics;
    private final ThulawaMetricsRecorder thulawaMetricsRecorder;

    private ThulawaTaskManager.State state;

    public ThulawaTaskManager(ThreadPoolRegistry threadPoolRegistry, ThulawaMetrics thulawaMetrics, ThulawaMetricsRecorder thulawaMetricsRecorder) {
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaMetrics = thulawaMetrics;
        this.thulawaMetricsRecorder = thulawaMetricsRecorder;
        this.state = State.CREATED;
    }

    static {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(new Options().setCreateIfMissing(true), "rocksdb_tasks");
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB", e);
        }
    }

    /**
     * Adds a task to the queue of active tasks for a specific thread.
     * Creates a new task queue for the thread if it doesn't already exist.
     *
     * @param threadPoolName The name of the thread.
     * @param thulawaTask    The task to add.
     */
    public void addActiveTask(String threadPoolName, ThulawaTask thulawaTask) {
        // Ensure the thread has a task queue
        assignedActiveTasks.computeIfAbsent(threadPoolName, k -> new ConcurrentLinkedQueue<>());

        // Add the task to the thread's task queue
        assignedActiveTasks.get(threadPoolName).add(thulawaTask);

        if (state != State.ACTIVE) {
            startTaskManagerThread();
        }
    }

    public void addActiveTask(String threadName, String taskData) {
        try {
            db.put(threadName.getBytes(StandardCharsets.UTF_8), taskData.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error storing task", e);
        }
    }

    public String getActiveTask(String threadName) {
        try {
            byte[] value = db.get(threadName.getBytes(StandardCharsets.UTF_8));
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Error retrieving task", e);
        }
    }

    public void deleteTask(String threadName) {
        try {
            db.delete(threadName.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error deleting task", e);
        }
    }

    /**
     * Submits ThulawaTasks from assignedActiveTasks to the appropriate thread pools.
     */
    public void submitThulawaTasks() {
        synchronized (this) {
            // Check if there are tasks to process
            if (assignedActiveTasks.isEmpty()) {
                logger.info("No tasks to process.");
                return;
            }

            while (this.state == State.ACTIVE) {
                for (String threadPoolName : assignedActiveTasks.keySet()) {
                    Queue<ThulawaTask> taskQueue = assignedActiveTasks.get(threadPoolName);

                    ThulawaTask task = taskQueue.poll();
                    if (task != null) {
                        int processedRecordCount = task.getRecords().size();

                        // Log metrics based on priority
                        if (Objects.equals(task.getPriority(), "high-priority")) {
                            thulawaMetricsRecorder.updateHighPriorityTasks(processedRecordCount);
                        } else {
                            thulawaMetricsRecorder.updateLowPriorityTasks(processedRecordCount);
                        }

                        // Submit the task to the appropriate thread pool
                        threadPoolRegistry.getThreadPool(threadPoolName).submit(task.getRunnableProcess());
                    }
                }
            }
        }
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
            this.threadPoolRegistry.getThreadPool(THULAWA_TASK_MANAGER_THREAD_POOL).submit(this::submitThulawaTasks);
            this.state = State.ACTIVE;
        }
    }

    private enum State {
        CREATED,
        ACTIVE
    }

}
