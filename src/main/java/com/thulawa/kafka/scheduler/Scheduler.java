package com.thulawa.kafka.scheduler;

public interface Scheduler {

    void notifyScheduler();
    boolean isActive();

}
