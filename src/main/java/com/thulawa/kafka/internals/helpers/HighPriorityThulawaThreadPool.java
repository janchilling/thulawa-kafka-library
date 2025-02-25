package com.thulawa.kafka.internals.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HighPriorityThulawaThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(HighPriorityThulawaThreadPool.class);

    // Prefix for threads in this pool
    public static final String THULAWA_THREAD_NAME = "HighPriorityThulawaThread-%d";

    private final ThreadPoolExecutor threadPool;
    private final AtomicInteger threadNameIndex = new AtomicInteger(0);

    /**
     * Constructor to initialize the thread pool with a fixed number of threads and a bounded task queue.
     *
     * @param poolSize  The number of worker threads in the pool.
     * @param queueSize The size of the task queue.
     */
    public HighPriorityThulawaThreadPool(int poolSize, int queueSize) {
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(queueSize);
        threadPool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                taskQueue,
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName(generateThreadName(threadNameIndex.getAndIncrement()));
                    return thread;
                }
        );

        logger.info("HighPriorityThulawaThread initialized with poolSize: {} and queueSize: {}", poolSize, queueSize);
    }

    /**
     * Submits a batch task to the thread pool for processing.
     *
     * @param task The batch task to execute.
     */
    public void submitTask(Runnable task) {
        threadPool.execute(task);
    }

    /**
     * Retrieves the names of all active threads in the thread pool.
     *
     * @return A set of active thread names.
     */
    public Set<String> getActiveThreadNames() {
        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);

        // Collect names of threads belonging to this pool
        return Arrays.stream(threads)
                .filter(thread -> thread != null && thread.getName().startsWith("ThulawaThread-"))
                .map(Thread::getName)
                .collect(Collectors.toSet());
    }

    /**
     * Shuts down the thread pool gracefully, allowing currently running tasks to finish.
     */
    public void shutdown() {
        logger.info("Shutting down ThulawaWorkerThreadPool...");
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warn("Forcing shutdown as tasks did not complete in time.");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Thread pool shutdown interrupted. Forcing shutdown.", e);
            threadPool.shutdownNow();
        }
    }

    /**
     * Generates a unique name for each thread in the pool.
     *
     * @param index The index of the thread.
     * @return The generated thread name.
     */
    private static String generateThreadName(int index) {
        return String.format(THULAWA_THREAD_NAME, index);
    }
}
