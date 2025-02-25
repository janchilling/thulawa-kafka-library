package com.thulawa.kafka.internals.storage;

import org.apache.kafka.streams.processor.api.Record;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;

import java.nio.charset.StandardCharsets;

public class KeyBasedQueue {
    private static RocksDB db;
    private final String recordKey;

    static {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(new Options().setCreateIfMissing(true), "rocksdb_data");
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB", e);
        }
    }

    public KeyBasedQueue(String recordKey) {
        this.recordKey = recordKey;
    }

    public void addRecord(Record record) {
        try {
            db.put(recordKey.getBytes(StandardCharsets.UTF_8), record.toString().getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error storing record", e);
        }
    }

    public String fetchRecord() {
        try {
            byte[] value = db.get(recordKey.getBytes(StandardCharsets.UTF_8));
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Error retrieving record", e);
        }
    }

    public void deleteRecord() {
        try {
            db.delete(recordKey.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error deleting record", e);
        }
    }
}
