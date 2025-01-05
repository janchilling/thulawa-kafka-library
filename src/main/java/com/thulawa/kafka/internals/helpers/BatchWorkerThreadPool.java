package com.thulawa.kafka.internals.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BatchWorkerThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(BatchWorkerThreadPool.class);

    private final ThreadPoolExecutor threadPool;

    /**
     * Constructor to initialize the thread pool with a fixed number of threads and a bounded task queue.
     *
     * @param poolSize The number of worker threads in the pool.
     * @param queueSize The size of the task queue.
     */
    public BatchWorkerThreadPool(int poolSize, int queueSize) {
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(queueSize);
        threadPool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                taskQueue,
                new ThreadPoolExecutor.CallerRunsPolicy() // Handle rejected tasks
        );

        logger.info("BatchWorkerThreadPool initialized with poolSize: {} and queueSize: {}", poolSize, queueSize);
    }

    /**
     * Submits a batch task to the thread pool for processing.
     *
     * @param task The batch task to execute.
     */
    public void submitBatchTask(Runnable task) {
        threadPool.execute(task);
    }

    /**
     * Shuts down the thread pool gracefully, allowing currently running tasks to finish.
     */
    public void shutdown() {
        logger.info("Shutting down BatchWorkerThreadPool...");
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
}
