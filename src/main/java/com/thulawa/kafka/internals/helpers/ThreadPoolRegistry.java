package com.thulawa.kafka.internals.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread manager where:
 * - Scheduler and Task Manager run as dedicated threads (not in a thread pool)
 * - Executor uses a configurable thread pool
 */
public class ThreadPoolRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolRegistry.class);
    private static ThreadPoolRegistry instance;

    public static final String THULAWA_EXECUTOR_THREAD_POOL = "Thulawa-Executor-Thread-Pool";

    private final Map<String, Thread> dedicatedThreads = new ConcurrentHashMap<>();
    private final Map<String, ThreadPoolExecutor> threadPools = new ConcurrentHashMap<>();

    private ThreadPoolRegistry(int threadPoolSize,
                               boolean threadPoolEnabled) {
        if (threadPoolEnabled){
            registerExecutorThreadPool(THULAWA_EXECUTOR_THREAD_POOL, threadPoolSize, threadPoolSize, 500);
        }
    }

    public static synchronized ThreadPoolRegistry getInstance(int threadPoolSize,
                                                              boolean threadPoolEnabled) {
        if (instance == null) {
            instance = new ThreadPoolRegistry(threadPoolSize, threadPoolEnabled);
        }
        return instance;
    }

    // ------------------- Dedicated Threads -------------------

    public void startDedicatedThread(String name, Runnable task) {
        if (dedicatedThreads.containsKey(name)) {
            throw new IllegalArgumentException("Thread with name " + name + " already exists.");
        }

        Thread thread = new Thread(task);
        thread.setName(name);
        thread.setDaemon(true);
        thread.start();

        dedicatedThreads.put(name, thread);
        logger.info("Started dedicated thread: {}", name);
    }

    public void stopDedicatedThread(String name) {
        Thread thread = dedicatedThreads.remove(name);
        if (thread != null) {
            thread.interrupt();
            logger.info("Stopped dedicated thread: {}", name);
        }
    }

    public void stopAllDedicatedThreads() {
        dedicatedThreads.forEach((name, thread) -> {
            thread.interrupt();
            logger.info("Stopped dedicated thread: {}", name);
        });
        dedicatedThreads.clear();
    }

    // ------------------- Executor Thread Pool -------------------

    public void registerExecutorThreadPool(String name, int corePoolSize, int maxPoolSize, int queueSize) {
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
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        threadPools.put(name, threadPool);
        logger.info("Registered executor thread pool: {}", name);
    }

    public ThreadPoolExecutor getThreadPool(String name) {
        return threadPools.get(name);
    }

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
            logger.info("Shutdown executor thread pool: {}", name);
        }
    }

    public void shutdownAllThreadPools() {
        threadPools.forEach((name, pool) -> {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Shutdown executor thread pool: {}", name);
        });
        threadPools.clear();
    }

    // ------------------- Unified Shutdown -------------------

    public void shutdownAll() {
        stopAllDedicatedThreads();
        shutdownAllThreadPools();
        logger.info("ExecutionRegistry shutdown complete.");
    }
}
