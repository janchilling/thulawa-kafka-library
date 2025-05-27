package com.thulawa.kafka;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ThulawaTaskManagerTest {

    private ThulawaTaskManager taskManager;
    private ThreadPoolRegistry threadPoolRegistry;
    private ThulawaMetrics thulawaMetrics;
    private MicroBatcher microBatcher;
    private ThulawaMetricsRecorder metricsRecorder;
    private QueueManager queueManager;

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        // Reset all singleton instances before each test
        resetSingletons();

        // Create unique instances for each test
        String testName = testInfo.getDisplayName();
        int threadPoolSize = 5;

        // Initialize dependencies with fresh instances
        threadPoolRegistry = ThreadPoolRegistry.getInstance(threadPoolSize, true);
        thulawaMetrics = new ThulawaMetrics(new Metrics());
        queueManager = QueueManager.getInstance(new HashSet<>());
        microBatcher = new MicroBatcher(queueManager);
        metricsRecorder = new ThulawaMetricsRecorder(thulawaMetrics);

        // Create task manager with thread pool enabled
        taskManager = new ThulawaTaskManager(
                threadPoolRegistry,
                thulawaMetrics,
                microBatcher,
                metricsRecorder,
                true
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean shutdown
        if (threadPoolRegistry != null) {
            threadPoolRegistry.shutdownAll();
        }
        if (thulawaMetrics != null) {
            thulawaMetrics.close();
        }

        // Reset singletons after each test
        resetSingletons();

        // Give some time for cleanup
        Thread.sleep(100);
    }

    /**
     * Reset singleton instances using reflection to ensure test isolation
     */
    private void resetSingletons() throws Exception {
        // Reset ThreadPoolRegistry singleton
        Field threadPoolRegistryInstance = ThreadPoolRegistry.class.getDeclaredField("instance");
        threadPoolRegistryInstance.setAccessible(true);
        threadPoolRegistryInstance.set(null, null);

        // Reset QueueManager singleton
        Field queueManagerInstance = QueueManager.class.getDeclaredField("instance");
        queueManagerInstance.setAccessible(true);
        queueManagerInstance.set(null, null);
    }

    @Test
    void testAddActiveTask_SingleTask() {
        // Test 1: Adding a single task to the manager
        String key = "test-key-1";
        ThulawaEvent event = createTestEvent("test-value");
        ThulawaTask task = new ThulawaTask("test-pool", Arrays.asList(event));

        taskManager.addActiveTask(key, task);

        // Verify task was added (success count should be 0 initially)
        assertEquals(0, taskManager.getSuccessCount(key));
        assertEquals(0, taskManager.getTotalSuccessCount());
    }

    @Test
    void testAddActiveTask_MultipleTasksSameKey() {
        // Test 2: Adding multiple tasks with the same key
        String key = "test-key-2";
        ThulawaEvent event1 = createTestEvent("value1");
        ThulawaEvent event2 = createTestEvent("value2");

        ThulawaTask task1 = new ThulawaTask("test-pool", Arrays.asList(event1));
        ThulawaTask task2 = new ThulawaTask("test-pool", Arrays.asList(event2));

        taskManager.addActiveTask(key, task1);
        taskManager.addActiveTask(key, task2);

        // Both tasks should be queued for the same key
        assertEquals(0, taskManager.getSuccessCount(key));
    }

    @Test
    void testAddActiveTask_DifferentKeys() {
        // Test 3: Adding tasks with different keys
        String key1 = "test-key-3a";
        String key2 = "test-key-3b";

        ThulawaEvent event1 = createTestEvent("value1");
        ThulawaEvent event2 = createTestEvent("value2");

        ThulawaTask task1 = new ThulawaTask("test-pool", Arrays.asList(event1));
        ThulawaTask task2 = new ThulawaTask("test-pool", Arrays.asList(event2));

        taskManager.addActiveTask(key1, task1);
        taskManager.addActiveTask(key2, task2);

        // Each key should have its own counter
        assertEquals(0, taskManager.getSuccessCount(key1));
        assertEquals(0, taskManager.getSuccessCount(key2));
    }

    @Test
    void testTaskExecution_SingleEvent() throws InterruptedException {
        // Test 4: Execute a task with a single event
        String key = "test-key-4";
        AtomicInteger executionCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Runnable testRunnable = () -> {
            executionCounter.incrementAndGet();
            latch.countDown();
        };

        ThulawaEvent event = createTestEventWithRunnable(testRunnable);
        ThulawaTask task = new ThulawaTask("test-pool", Arrays.asList(event));

        taskManager.addActiveTask(key, task);

        // Start the task manager thread explicitly
        taskManager.startTaskManagerThread();

        // Wait for task execution
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");
        assertEquals(1, executionCounter.get());

        // Give some time for success count to be updated
        Thread.sleep(500);
        assertEquals(1, taskManager.getSuccessCount(key));
    }

    @Test
    void testTaskExecution_MultipleEvents() throws InterruptedException {
        // Test 5: Execute a task with multiple events
        String key = "test-key-5";
        AtomicInteger executionCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        Runnable testRunnable = () -> {
            executionCounter.incrementAndGet();
            latch.countDown();
        };

        ThulawaEvent event1 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event2 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event3 = createTestEventWithRunnable(testRunnable);

        ThulawaTask task = new ThulawaTask("test-pool", Arrays.asList(event1, event2, event3));

        taskManager.addActiveTask(key, task);

        // Start the task manager thread explicitly
        taskManager.startTaskManagerThread();

        // Wait for all events to execute
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");
        assertEquals(3, executionCounter.get());

        // Give some time for success count to be updated
        Thread.sleep(500);
        assertEquals(3, taskManager.getSuccessCount(key));
    }

    @Test
    void testSuccessCountTracking() throws InterruptedException {
        // Test 6: Verify success count tracking across multiple tasks
        String key = "test-key-6";
        CountDownLatch latch = new CountDownLatch(5);

        Runnable testRunnable = () -> latch.countDown();

        // First task with 2 events
        ThulawaEvent event1 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event2 = createTestEventWithRunnable(testRunnable);
        ThulawaTask task1 = new ThulawaTask("test-pool", Arrays.asList(event1, event2));

        // Second task with 3 events
        ThulawaEvent event3 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event4 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event5 = createTestEventWithRunnable(testRunnable);
        ThulawaTask task2 = new ThulawaTask("test-pool", Arrays.asList(event3, event4, event5));

        taskManager.addActiveTask(key, task1);
        taskManager.addActiveTask(key, task2);

        // Start the task manager thread explicitly
        taskManager.startTaskManagerThread();

        // Wait for all events to execute
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");

        // Give some time for success count to be updated
        Thread.sleep(1000);
        assertEquals(5, taskManager.getSuccessCount(key));
        assertEquals(5, taskManager.getTotalSuccessCount());
    }

    @Test
    void testTotalSuccessCountAcrossKeys() throws InterruptedException {
        // Test 7: Verify total success count across different keys
        CountDownLatch latch = new CountDownLatch(4);
        Runnable testRunnable = () -> latch.countDown();

        // Key 1: 2 events
        String key1 = "test-key-7a";
        ThulawaEvent event1 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event2 = createTestEventWithRunnable(testRunnable);
        ThulawaTask task1 = new ThulawaTask("test-pool", Arrays.asList(event1, event2));

        // Key 2: 2 events
        String key2 = "test-key-7b";
        ThulawaEvent event3 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event4 = createTestEventWithRunnable(testRunnable);
        ThulawaTask task2 = new ThulawaTask("test-pool", Arrays.asList(event3, event4));

        taskManager.addActiveTask(key1, task1);
        taskManager.addActiveTask(key2, task2);

        // Start the task manager thread explicitly
        taskManager.startTaskManagerThread();

        // Wait for all events to execute
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");

        // Give some time for success count to be updated
        Thread.sleep(1000);
        assertEquals(2, taskManager.getSuccessCount(key1));
        assertEquals(2, taskManager.getSuccessCount(key2));
        assertEquals(4, taskManager.getTotalSuccessCount());
    }

    @Test
    void testTaskExecutionWithException() throws InterruptedException {
        // Test 8: Handle task execution with exceptions
        String key = "test-key-8";
        CountDownLatch latch = new CountDownLatch(1);

        Runnable failingRunnable = () -> {
            latch.countDown();
            throw new RuntimeException("Test exception");
        };

        ThulawaEvent event = createTestEventWithRunnable(failingRunnable);
        ThulawaTask task = new ThulawaTask("test-pool", Arrays.asList(event));

        taskManager.addActiveTask(key, task);

        // Start the task manager thread explicitly
        taskManager.startTaskManagerThread();

        // Wait for task to be attempted
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");

        // Give some time for processing
        Thread.sleep(1000);

        // Success count should remain 0 due to exception
        assertEquals(0, taskManager.getSuccessCount(key));
        assertEquals(0, taskManager.getTotalSuccessCount());
    }

    @Test
    void testVirtualThreadExecution() throws InterruptedException {
        // Test 9: Test with virtual threads (thread pool disabled)
        // Create a separate ThreadPoolRegistry for this test
        ThreadPoolRegistry virtualThreadRegistry = ThreadPoolRegistry.getInstance(0, false);

        ThulawaTaskManager virtualThreadManager = new ThulawaTaskManager(
                virtualThreadRegistry,
                thulawaMetrics,
                microBatcher,
                metricsRecorder,
                false // Thread pool disabled - uses virtual threads
        );

        String key = "test-key-9";
        CountDownLatch latch = new CountDownLatch(2);

        Runnable testRunnable = () -> latch.countDown();

        ThulawaEvent event1 = createTestEventWithRunnable(testRunnable);
        ThulawaEvent event2 = createTestEventWithRunnable(testRunnable);
        ThulawaTask task = new ThulawaTask("test-pool", Arrays.asList(event1, event2));

        virtualThreadManager.addActiveTask(key, task);

        // Start the virtual thread task manager explicitly
        virtualThreadManager.startTaskManagerThread();

        // Wait for task execution
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Task execution timed out");

        // Give some time for success count to be updated
        Thread.sleep(1000);
        assertEquals(2, virtualThreadManager.getSuccessCount(key));
    }

    // Helper methods
    private ThulawaEvent createTestEvent(String value) {
        Record<String, String> record = new Record<>("test-key", value, System.currentTimeMillis());
        Runnable emptyRunnable = () -> {}; // Empty runnable for basic tests
        return new ThulawaEvent(record, System.currentTimeMillis(), emptyRunnable);
    }

    private ThulawaEvent createTestEventWithRunnable(Runnable runnable) {
        Record<String, String> record = new Record<>("test-key", "test-value", System.currentTimeMillis());
        return new ThulawaEvent(record, System.currentTimeMillis(), runnable);
    }
}