package com.thulawa.kafka.internals.storage;

import com.thulawa.kafka.ThulawaTask;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * ActiveTaskQueue is a thread-safe queue that holds active ThulawaTasks.
 * It ensures that multiple threads can safely add and remove tasks concurrently.
 */
public class ActiveTaskQueue {

    private final Queue<ThulawaTask> taskQueue;

    public ActiveTaskQueue() {
        this.taskQueue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Adds a task to the queue.
     * @param task The ThulawaTask to be added.
     */
    public void addTask(ThulawaTask task) {
        if (task != null) {
            taskQueue.offer(task);
        }
    }

    /**
     * Retrieves and removes the next available task from the queue.
     * @return The next ThulawaTask or null if the queue is empty.
     */
    public ThulawaTask pollTask() {
        return taskQueue.poll();
    }

    /**
     * Checks if the queue is empty.
     * @return true if empty, otherwise false.
     */
    public boolean isEmpty() {
        return taskQueue.isEmpty();
    }

    /**
     * Gets the current size of the queue.
     * @return The number of tasks in the queue.
     */
    public int size() {
        return taskQueue.size();
    }
}
