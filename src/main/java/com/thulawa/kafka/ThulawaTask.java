package com.thulawa.kafka;

import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

public class ThulawaTask {

    private final String threadPoolName;
    private final List<Record> records;

    private final String priority;
    private final Runnable runnableProcess;

    public ThulawaTask(String threadPoolName, List<Record> records, String priority, Runnable runnableProcess) {
        this.threadPoolName = threadPoolName;
        this.records = records;
        this.priority = priority;
        this.runnableProcess = runnableProcess;
    }

    public List<Record> getRecords() {
        return records;
    }

    public String getPriority() {
        return priority;
    }

    public Runnable getRunnableProcess() {
        return runnableProcess;
    }
}
