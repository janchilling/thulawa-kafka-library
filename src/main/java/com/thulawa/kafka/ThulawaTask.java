package com.thulawa.kafka;

import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

public class ThulawaTask {

    private final String threadPoolName;
    private final List<ThulawaEvent> thulawaEvents;

    public ThulawaTask(String threadPoolName,
                       List<ThulawaEvent> thulawaEvents) {
        this.threadPoolName = threadPoolName;
        this.thulawaEvents = thulawaEvents;
    }

    public List<ThulawaEvent> getThulawaEvents() {
        return thulawaEvents;
    }
}
