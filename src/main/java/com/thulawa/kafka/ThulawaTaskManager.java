package com.thulawa.kafka;

import org.rocksdb.*;
import java.nio.charset.StandardCharsets;

public class ThulawaTaskManager {
    private static RocksDB db;

    static {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(new Options().setCreateIfMissing(true), "rocksdb_tasks");
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB", e);
        }
    }

    public void addActiveTask(String threadName, String taskData) {
        try {
            db.put(threadName.getBytes(StandardCharsets.UTF_8), taskData.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error storing task", e);
        }
    }

    public String getActiveTask(String threadName) {
        try {
            byte[] value = db.get(threadName.getBytes(StandardCharsets.UTF_8));
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Error retrieving task", e);
        }
    }

    public void deleteTask(String threadName) {
        try {
            db.delete(threadName.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new RuntimeException("Error deleting task", e);
        }
    }
}
