package com.thulawa.kafka;

import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

public class ThulawaTask {

    private final String threadPoolName;
    private final List<Record> records;
    private final Runnable runnableProcess;

    public ThulawaTask(String threadPoolName, List<Record> records, Runnable runnableProcess) {
        this.threadPoolName = threadPoolName;
        this.records = records;
        this.runnableProcess = runnableProcess;
    }

    public Runnable getRunnableProcess() {
        return runnableProcess;
    }
}
