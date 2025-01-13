package com.thulawa.kafka;

import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * ThulawaTaskManager handles the management of active tasks assigned to threads.
 */
public class ThulawaTaskManager {

    // Map of thread names to their assigned active tasks
    private final Map<String, Queue<ThulawaTask>> assignedActiveTasks = new ConcurrentHashMap<>();

    private final ThreadPoolRegistry threadPoolRegistry;

    public ThulawaTaskManager(ThreadPoolRegistry threadPoolRegistry) {
        this.threadPoolRegistry = threadPoolRegistry;
    }

    /**
     * Adds a task to the queue of active tasks for a specific thread.
     * Creates a new task queue for the thread if it doesn't already exist.
     *
     * @param threadPoolName  The name of the thread.
     * @param thulawaTask The task to add.
     */
    public void addActiveTask(String threadPoolName, ThulawaTask thulawaTask) {
        // Ensure the thread has a task queue
        assignedActiveTasks.computeIfAbsent(threadPoolName, k -> new ConcurrentLinkedQueue<>());
        // Add the task to the thread's task queue
        assignedActiveTasks.get(threadPoolName).add(thulawaTask);
        ohShootATaskWasAdded(threadPoolName);


    }

    /**
     * Removes a task from the queue of active tasks for a specific thread.
     *
     * @param threadPoolName  The name of the thread.
     * @param thulawaTask The task to remove.
     * @return True if the task was removed, false otherwise.
     */
    public boolean removeActiveTask(String threadPoolName, ThulawaTask thulawaTask) {
        Queue<ThulawaTask> tasks = assignedActiveTasks.get(threadPoolName);
        if (tasks != null) {
            boolean removed = tasks.remove(thulawaTask);
            // Remove the thread entry if the queue is now empty
            if (tasks.isEmpty()) {
                assignedActiveTasks.remove(threadPoolName);
            }
            return removed;
        }
        return false;
    }

    /**
     * Retrieves and removes the next task from the queue for a specific thread.
     *
     * @param threadPoolName The name of the thread.
     * @return The next task, or null if no tasks are available for the thread.
     */
    public ThulawaTask pollActiveTask(String threadPoolName) {
        Queue<ThulawaTask> tasks = assignedActiveTasks.get(threadPoolName);
        if (tasks != null) {
            ThulawaTask task = tasks.poll();
            // Remove the thread entry if the queue is now empty
            if (tasks.isEmpty()) {
                assignedActiveTasks.remove(threadPoolName);
            }
            return task;
        }
        return null;
    }

    /**
     * Retrieves all active tasks assigned to a specific thread.
     *
     * @param threadPoolName The name of the thread.
     * @return A queue of tasks assigned to the thread, or null if no tasks are assigned.
     */
    public Queue<ThulawaTask> getActiveTasks(String threadPoolName) {
        return assignedActiveTasks.get(threadPoolName);
    }

    /**
     * Checks if a thread has any active tasks.
     *
     * @param threadPoolName The name of the thread.
     * @return True if the thread has active tasks, false otherwise.
     */
    public boolean hasActiveTasks(String threadPoolName) {
        Queue<ThulawaTask> tasks = assignedActiveTasks.get(threadPoolName);
        return tasks != null && !tasks.isEmpty();
    }

    /**
     * Clears all active tasks for a specific thread.
     *
     * @param threadPoolName The name of the thread.
     */
    public void clearActiveTasks(String threadPoolName) {
        assignedActiveTasks.remove(threadPoolName);
    }

    /**
     * Clears all active tasks for all threads.
     */
    public void clearAllTasks() {
        assignedActiveTasks.clear();
    }

    /**
     * Gets the total number of active tasks across all threads.
     *
     * @return The total number of active tasks.
     */
    public int getTotalActiveTasks() {
        return assignedActiveTasks.values().stream()
                .mapToInt(Queue::size)
                .sum();
    }

    public void ohShootATaskWasAdded(String threadPoolName) {
        ThulawaTask task = assignedActiveTasks.get(threadPoolName).poll();
        if (task != null) {
            threadPoolRegistry.getThreadPool(threadPoolName).submit(task.getRunnableProcess());
        }
    }


}
