package com.thulawa.kafka.scheduler;

import com.thulawa.kafka.ThulawaTask;
import com.thulawa.kafka.ThulawaTaskManager;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import org.apache.kafka.streams.processor.api.Processor;

public class ThulawaScheduler {

    private final QueueManager queueManager;
    private final ThreadPoolRegistry threadPoolRegistry;
    private final ThulawaTaskManager thulawaTaskManager;
    private final Processor processor;

    private State state;

    public ThulawaScheduler(QueueManager queueManager, ThreadPoolRegistry threadPoolRegistry, ThulawaTaskManager thulawaTaskManager, Processor processor) {
        this.queueManager = queueManager;
        this.threadPoolRegistry = threadPoolRegistry;
        this.thulawaTaskManager = thulawaTaskManager;
        this.processor = processor;
        this.state = State.CREATED;
    }

    /**
     *  1. Listens for arriving events .
     *  2. Listen for JVM Metrics.
     *  3. Update the ThulawaTaskManager with the necessary tasks.
     *  eg - Dynamically manage the Runnable Task slots based on thread busyness.
     *  4. Create or Delete thread based on the requirements.
     *
     */
    public void schedule() {
        this.state = State.ACTIVE;

        while (this.state == State.ACTIVE) {
            try {
                int availableRecordsInQueues = queueManager.getAvailableRecordsInQueuesSize();

                if (availableRecordsInQueues > 0) {
                    ThulawaTask thulawaTask = new ThulawaTask("HighPriorityPool",
                            queueManager.getRecordFromQueue("__NULL_KEY__"),
                            () -> processor.process(queueManager.getRecordFromQueue("__NULL_KEY__")));
                    this.thulawaTaskManager.addActiveTask("HighPriorityPool", thulawaTask);
                }
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    private enum State{
        CREATED,
        ACTIVE,
        INACTIVE,
        DEAD
    }

}
