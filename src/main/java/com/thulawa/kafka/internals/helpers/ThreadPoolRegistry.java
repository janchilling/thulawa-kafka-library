package com.thulawa.kafka.internals.helpers;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolRegistry {

    private final Map<String, ThreadPoolExecutor> threadPools = new ConcurrentHashMap<>();

    /**
     * Creates and registers a thread pool with the given name, pool size, and queue size.
     *
     * @param name       The unique name of the thread pool.
     * @param poolSize   The number of threads in the pool.
     * @param queueSize  The size of the task queue.
     */
    public void registerThreadPool(String name, int poolSize, int queueSize) {
        if (threadPools.containsKey(name)) {
            throw new IllegalArgumentException("Thread pool with name " + name + " already exists.");
        }

        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(queueSize);
        AtomicInteger threadNameIndex = new AtomicInteger(0);

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                taskQueue,
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName(name + "-Thread-" + threadNameIndex.getAndIncrement());
                    return thread;
                }
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
