package com.thulawa.kafka.internals.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolRegistry.class);
    private static ThreadPoolRegistry instance;

    public static final String THULAWA_SCHEDULING_THREAD_POOL = "Thulawa-Scheduling-Thread-Pool";
    public static final String THULAWA_TASK_MANAGER_THREAD_POOL = "Thulawa-Task-Manager-Thread-Pool";
    public static final String THULAWA_EXECUTOR_THREAD_POOL = "Thulawa-Executor-Thread-Pool";

    private final Map<String, ThreadPoolExecutor> threadPools = new ConcurrentHashMap<>();

    private ThreadPoolRegistry() {
        // Register thread pools with better configurations
        this.registerThreadPool(THULAWA_SCHEDULING_THREAD_POOL, 2, 4, 100);
        this.registerThreadPool(THULAWA_TASK_MANAGER_THREAD_POOL, 2, 5, 100);
        this.registerThreadPool(THULAWA_EXECUTOR_THREAD_POOL, 2, 10, 500);
    }

    public static synchronized ThreadPoolRegistry getInstance() {
        if (instance == null) {
            instance = new ThreadPoolRegistry();
        }
        return instance;
    }

    /**
     * Creates and registers a thread pool with dynamic scaling.
     *
     * @param name         The unique name of the thread pool.
     * @param corePoolSize Minimum number of threads.
     * @param maxPoolSize  Maximum number of threads.
     * @param queueSize    The task queue size.
     */
    public void registerThreadPool(String name, int corePoolSize, int maxPoolSize, int queueSize) {
        if (threadPools.containsKey(name)) {
            throw new IllegalArgumentException("Thread pool with name " + name + " already exists.");
        }

        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(queueSize);
        AtomicInteger threadNameIndex = new AtomicInteger(0);

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L, TimeUnit.SECONDS,
                taskQueue,
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName(name + "-Thread-" + threadNameIndex.getAndIncrement());
                    thread.setDaemon(true);  // Ensure proper shutdown
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // Prevent task rejection
        );

        threadPools.put(name, threadPool);
    }

    /**
     * Retrieves a thread pool by name.
     *
     * @param name The name of the thread pool.
     * @return The thread pool executor.
     */
    public ThreadPoolExecutor getThreadPool(String name) {
        return threadPools.get(name);
    }

    /**
     * Shuts down all registered thread pools.
     */
    public void shutdownAll() {
        threadPools.forEach((name, threadPool) -> {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        threadPools.clear();
    }

    /**
     * Shuts down a specific thread pool by name.
     *
     * @param name The name of the thread pool.
     */
    public void shutdownThreadPool(String name) {
        ThreadPoolExecutor threadPool = threadPools.remove(name);
        if (threadPool != null) {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
