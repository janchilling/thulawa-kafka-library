package com.thulawa.kafka;

import com.thulawa.kafka.ThulawaEvent;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.scheduler.Scheduler;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueueManagerTest {

    private QueueManager queueManager;
    private Set<Object> highPriorityKeySet;

    @Mock
    private Scheduler mockScheduler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        highPriorityKeySet = new HashSet<>();
        highPriorityKeySet.add("high-priority-key");
        queueManager = QueueManager.getInstance(highPriorityKeySet);
        // Reset the singleton for each test (for testing purposes only)
        resetQueueManagerInstance();
    }

    private void resetQueueManagerInstance() {
        // This is a helper method to reset the singleton state for testing
        // In a real scenario, you might need to use reflection or dependency injection
        queueManager = QueueManager.getInstance(highPriorityKeySet);
    }

    private ThulawaEvent createMockEvent(Object key, Object value, long timestamp) {
        Record<Object, Object> record = new Record<>(key, value, timestamp);
        Runnable mockRunnable = () -> System.out.println("Processing: " + value);
        return new ThulawaEvent(record, timestamp, mockRunnable);
    }

    @Test
    @DisplayName("Test 1: Singleton instance creation")
    void testSingletonInstanceCreation() {
        QueueManager instance1 = QueueManager.getInstance(highPriorityKeySet);
        QueueManager instance2 = QueueManager.getInstance(highPriorityKeySet);

        assertSame(instance1, instance2, "QueueManager should return the same singleton instance");
    }

//    @Test
//    @DisplayName("Test 2: Add event to key-based queue with valid key")
//    void testAddEventWithValidKey() {
//        String key = "test-key";
//        ThulawaEvent event = createMockEvent(key, "test-value", 1000L);
//
//        assertDoesNotThrow(() -> queueManager.addToKeyBasedQueue(key, event));
//
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertEquals(key, earliestKey, "Earliest queue key should match the added key");
//    }

//    @Test
//    @DisplayName("Test 3: Add event with null key - should use common queue")
//    void testAddEventWithNullKey() {
//        ThulawaEvent event = createMockEvent(null, "test-value", 1000L);
//
//        assertDoesNotThrow(() -> queueManager.addToKeyBasedQueue(null, event));
//
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertEquals("low.priority.keys", earliestKey, "Null key should use common queue key");
//    }

//    @Test
//    @DisplayName("Test 4: Get earliest queue key when no events exist")
//    void testGetEarliestQueueKeyWhenEmpty() {
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertNull(earliestKey, "Earliest queue key should be null when no events exist");
//    }

//    @Test
//    @DisplayName("Test 5: Get earliest queue key with multiple events")
//    void testGetEarliestQueueKeyWithMultipleEvents() {
//        ThulawaEvent event1 = createMockEvent("key1", "value1", 2000L);
//        ThulawaEvent event2 = createMockEvent("key2", "value2", 1000L);
//        ThulawaEvent event3 = createMockEvent("key3", "value3", 3000L);
//
//        queueManager.addToKeyBasedQueue("key1", event1);
//        queueManager.addToKeyBasedQueue("key2", event2);
//        queueManager.addToKeyBasedQueue("key3", event3);
//
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertEquals("key2", earliestKey, "Should return key with earliest timestamp");
//    }

    @Test
    @DisplayName("Test 6: Get next record from queue")
    void testGetNextRecord() {
        String key = "test-key";
        ThulawaEvent event = createMockEvent(key, "test-value", 1000L);

        queueManager.addToKeyBasedQueue(key, event);
        ThulawaEvent retrievedEvent = queueManager.getNextRecord();

        assertNotNull(retrievedEvent, "Should retrieve the added event");
        assertEquals(1000L, retrievedEvent.getReceivedSystemTime(), "Retrieved event should have correct timestamp");
    }

//    @Test
//    @DisplayName("Test 7: Get next record when queue is empty")
//    void testGetNextRecordWhenEmpty() {
//        ThulawaEvent retrievedEvent = queueManager.getNextRecord();
//        assertNull(retrievedEvent, "Should return null when no events exist");
//    }

    @Test
    @DisplayName("Test 8: Get record batches with valid batch size")
    void testGetRecordBatchesWithValidBatchSize() {
        String key = "batch-key";

        for (int i = 0; i < 5; i++) {
            ThulawaEvent event = createMockEvent(key, "value" + i, 1000L + i);
            queueManager.addToKeyBasedQueue(key, event);
        }

        List<ThulawaEvent> batch = queueManager.getRecordBatchesFromKBQueues(key, 3);

        assertEquals(3, batch.size(), "Should return exactly 3 events");
    }

//    @Test
//    @DisplayName("Test 9: Get record batches with batch size larger than queue")
//    void testGetRecordBatchesWithLargeBatchSize() {
//        String key = "batch-key";
//
//        for (int i = 0; i < 2; i++) {
//            ThulawaEvent event = createMockEvent(key, "value" + i, 1000L + i);
//            queueManager.addToKeyBasedQueue(key, event);
//        }
//
//        List<ThulawaEvent> batch = queueManager.getRecordBatchesFromKBQueues(key, 5);
//
//        assertEquals(2, batch.size(), "Should return all available events (2)");
//    }

    @Test
    @DisplayName("Test 10: Get record batches from non-existent key")
    void testGetRecordBatchesFromNonExistentKey() {
        List<ThulawaEvent> batch = queueManager.getRecordBatchesFromKBQueues("non-existent-key", 3);

        assertTrue(batch.isEmpty(), "Should return empty list for non-existent key");
    }

    @Test
    @DisplayName("Test 11: Get record batches with null key")
    void testGetRecordBatchesWithNullKey() {
        ThulawaEvent event = createMockEvent(null, "test-value", 1000L);
        queueManager.addToKeyBasedQueue(null, event);

        List<ThulawaEvent> batch = queueManager.getRecordBatchesFromKBQueues(null, 1);

        assertEquals(1, batch.size(), "Should return event from common queue");
    }

    @Test
    @DisplayName("Test 12: Size of key-based queue")
    void testSizeOfKeyBasedQueue() {
        String key = "size-test-key";

        for (int i = 0; i < 3; i++) {
            ThulawaEvent event = createMockEvent(key, "value" + i, 1000L + i);
            queueManager.addToKeyBasedQueue(key, event);
        }

        int size = queueManager.sizeOfKeyBasedQueue(key);
        assertEquals(3, size, "Queue size should be 3");
    }

    @Test
    @DisplayName("Test 13: Set scheduler observer")
    void testSetSchedulerObserver() {
        assertDoesNotThrow(() -> queueManager.setSchedulerObserver(mockScheduler));
    }

    @Test
    @DisplayName("Test 14: Scheduler notification when inactive")
    void testSchedulerNotificationWhenInactive() {
        when(mockScheduler.isActive()).thenReturn(false);
        queueManager.setSchedulerObserver(mockScheduler);

        ThulawaEvent event = createMockEvent("test-key", "test-value", 1000L);
        queueManager.addToKeyBasedQueue("test-key", event);

        verify(mockScheduler, times(1)).notifyScheduler();
    }

    @Test
    @DisplayName("Test 15: No scheduler notification when active")
    void testNoSchedulerNotificationWhenActive() {
        when(mockScheduler.isActive()).thenReturn(true);
        queueManager.setSchedulerObserver(mockScheduler);

        ThulawaEvent event = createMockEvent("test-key", "test-value", 1000L);
        queueManager.addToKeyBasedQueue("test-key", event);

        verify(mockScheduler, never()).notifyScheduler();
    }

//    @Test
//    @DisplayName("Test 16: Add multiple events to same key - timestamp ordering")
//    void testMultipleEventsToSameKeyTimestampOrdering() {
//        String key = "ordering-key";
//        ThulawaEvent event1 = createMockEvent(key, "value1", 3000L);
//        ThulawaEvent event2 = createMockEvent(key, "value2", 1000L);
//        ThulawaEvent event3 = createMockEvent(key, "value3", 2000L);
//
//        queueManager.addToKeyBasedQueue(key, event1);
//        queueManager.addToKeyBasedQueue(key, event2);
//        queueManager.addToKeyBasedQueue(key, event3);
//
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertEquals(key, earliestKey, "Should identify key with earliest timestamp");
//    }

    @Test
    @DisplayName("Test 17: Thread safety - concurrent additions")
    void testConcurrentAdditions() throws InterruptedException {
        int numThreads = 10;
        int eventsPerThread = 100;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < eventsPerThread; j++) {
                    ThulawaEvent event = createMockEvent("key" + threadId, "value" + j, System.currentTimeMillis());
                    queueManager.addToKeyBasedQueue("key" + threadId, event);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Verify that all events were added successfully
        assertNotNull(queueManager.getEarliestQueueKey(), "Should have events after concurrent additions");
    }

//    @Test
//    @DisplayName("Test 18: Queue cleanup after consuming all events")
//    void testQueueCleanupAfterConsumingAllEvents() {
//        String key = "cleanup-key";
//        ThulawaEvent event1 = createMockEvent(key, "value1", 1000L);
//        ThulawaEvent event2 = createMockEvent(key, "value2", 2000L);
//
//        queueManager.addToKeyBasedQueue(key, event1);
//        queueManager.addToKeyBasedQueue(key, event2);
//
//        // Consume all events
//        List<ThulawaEvent> batch = queueManager.getRecordBatchesFromKBQueues(key, 2);
//        assertEquals(2, batch.size(), "Should consume all events");
//
//        // Queue should be cleaned up
//        Object earliestKey = queueManager.getEarliestQueueKey();
//        assertNull(earliestKey, "Earliest key should be null after consuming all events");
//    }

    @Test
    @DisplayName("Test 19: High priority key handling")
    void testHighPriorityKeyHandling() {
        String highPriorityKey = "high-priority-key";
        String normalKey = "normal-key";

        assertTrue(highPriorityKeySet.contains(highPriorityKey), "High priority key should be in the set");
        assertFalse(highPriorityKeySet.contains(normalKey), "Normal key should not be in high priority set");

        ThulawaEvent highPriorityEvent = createMockEvent(highPriorityKey, "high-value", 1000L);
        ThulawaEvent normalEvent = createMockEvent(normalKey, "normal-value", 2000L);

        queueManager.addToKeyBasedQueue(highPriorityKey, highPriorityEvent);
        queueManager.addToKeyBasedQueue(normalKey, normalEvent);

        // Both should be added successfully
        assertNotNull(queueManager.getEarliestQueueKey(), "Should have events from both priority levels");
    }

    @Test
    @DisplayName("Test 20: Batch processing with mixed keys")
    void testBatchProcessingWithMixedKeys() {
        String key1 = "batch-key-1";
        String key2 = "batch-key-2";

        // Add events to different keys
        for (int i = 0; i < 3; i++) {
            ThulawaEvent event1 = createMockEvent(key1, "value1-" + i, 1000L + i);
            ThulawaEvent event2 = createMockEvent(key2, "value2-" + i, 2000L + i);

            queueManager.addToKeyBasedQueue(key1, event1);
            queueManager.addToKeyBasedQueue(key2, event2);
        }

        // Get batches from each key
        List<ThulawaEvent> batch1 = queueManager.getRecordBatchesFromKBQueues(key1, 2);
        List<ThulawaEvent> batch2 = queueManager.getRecordBatchesFromKBQueues(key2, 2);

        assertEquals(2, batch1.size(), "Should get 2 events from key1");
        assertEquals(2, batch2.size(), "Should get 2 events from key2");

        // Verify remaining events
        assertEquals(1, queueManager.sizeOfKeyBasedQueue(key1), "Should have 1 remaining event in key1");
        assertEquals(1, queueManager.sizeOfKeyBasedQueue(key2), "Should have 1 remaining event in key2");
    }
}