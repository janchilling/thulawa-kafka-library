package com.thulawa.kafka.internals.storage;

import org.apache.kafka.streams.processor.api.Record;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Queue {

    private final ConcurrentLinkedQueue<Record> records= new ConcurrentLinkedQueue<>();

    public void add(Record record) {
        records.offer(record);
    }

    public Record<Object, Object> poll() {
        return records.poll();
    }

    public int size() {
        return records.size();
    }
}
