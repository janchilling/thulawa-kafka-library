package com.thulawa.kafka.internals.helpers;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class QueueManager {
    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    private static QueueManager instance;
    private static RocksDB db;
    private static final String DB_PATH = "rocksdb_data";

    private final Map<String, Integer> queueSizeMap = new ConcurrentHashMap<>();

    static {
        RocksDB.loadLibrary();
        try {
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, DB_PATH);
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB", e);
        }
    }

    private QueueManager() {}

    public static QueueManager getInstance() {
        if (instance == null) {
            instance = new QueueManager();
        }
        return instance;
    }

    public void addToKeyBasedQueue(String key, String record) {
        try {
            db.put(key.getBytes(StandardCharsets.UTF_8), record.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            logger.error("Failed to insert record into RocksDB", e);
        }
    }

    public String getRecordFromQueue(String key) {
        try {
            byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        } catch (RocksDBException e) {
            logger.error("Failed to fetch record from RocksDB", e);
            return null;
        }
    }

    public void deleteKey(String key) {
        try {
            db.delete(key.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            logger.error("Failed to delete key from RocksDB", e);
        }
    }
}
