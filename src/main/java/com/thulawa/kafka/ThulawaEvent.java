package com.thulawa.kafka;

import org.apache.kafka.streams.processor.api.Record;

public class ThulawaEvent {

    private final Object recordKey;
    private final Object recordValue;
    private final long receivedSystemTime;
    private final Runnable runnableProcess;


    public ThulawaEvent(Record<?, ?> record,
                        long receivedSystemTime,
                        Runnable runnableProcess) {
        this.recordKey = record.key();
        this.recordValue = record.value();
        this.receivedSystemTime = receivedSystemTime;
        this.runnableProcess = runnableProcess;
    }

    public long getReceivedSystemTime() {
        return receivedSystemTime;
    }

    public Runnable getRunnableProcess() {
        return runnableProcess;
    }

    public Object getRecordValue() {
        return recordValue;
    }
}
