package com.thulawa.kafka;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.scheduler.ThulawaScheduler;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ThulawaSchedulerTest {

    @Mock
    private QueueManager queueManager;

    @Mock
    private ThreadPoolRegistry threadPoolRegistry;

    @Mock
    private ThulawaTaskManager thulawaTaskManager;

    @Mock
    private ThulawaMetrics thulawaMetrics;

    @Mock
    private MicroBatcher microBatcher;

    private Set<String> highPriorityKeySet;
    private ThulawaScheduler scheduler;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        highPriorityKeySet = new HashSet<>(Arrays.asList("priority-key-1", "priority-key-2"));

        // Reset singleton instance before each test
        resetSingletonInstance();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
        resetSingletonInstance();
    }

    private void resetSingletonInstance() {
        try {
            Field instanceField = ThulawaScheduler.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, null);
        } catch (Exception e) {
            // Ignore exceptions during cleanup
        }
    }

    @Test
    @DisplayName("Test singleton instance creation")
    void testSingletonInstanceCreation() {
        // When
        ThulawaScheduler scheduler1 = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        ThulawaScheduler scheduler2 = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // Then
        assertNotNull(scheduler1);
        assertSame(scheduler1, scheduler2, "Should return the same singleton instance");
    }

    @Test
    @DisplayName("Test scheduler initialization with micro batcher enabled")
    void testSchedulerInitializationWithMicroBatcherEnabled() {
        // When
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // Then
        assertNotNull(scheduler);
        assertFalse(scheduler.isActive(), "Scheduler should not be active initially");
        verify(queueManager).setSchedulerObserver(scheduler);
    }

    @Test
    @DisplayName("Test scheduler initialization with micro batcher disabled")
    void testSchedulerInitializationWithMicroBatcherDisabled() {
        // When
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, false
        );

        // Then
        assertNotNull(scheduler);
        assertFalse(scheduler.isActive(), "Scheduler should not be active initially");
        verify(queueManager).setSchedulerObserver(scheduler);
    }

    @Test
    @DisplayName("Test notify scheduler when not active")
    void testNotifySchedulerWhenNotActive() {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // When
        scheduler.notifyScheduler();

        // Then
        verify(threadPoolRegistry).startDedicatedThread(
                eq("Thulawa-Scheduler-Thread"),
                any(Runnable.class)
        );
    }

    @Test
    @DisplayName("Test notify scheduler when already active")
    void testNotifySchedulerWhenAlreadyActive() {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // First activation
        scheduler.notifyScheduler();

        // When - second notification
        scheduler.notifyScheduler();

        // Then - should only start thread once
        verify(threadPoolRegistry, times(1)).startDedicatedThread(
                eq("Thulawa-Scheduler-Thread"),
                any(Runnable.class)
        );
    }

    @Test
    @DisplayName("Test process batch with micro batcher disabled")
    void testProcessBatchWithMicroBatcherDisabled() throws Exception {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, false
        );

        Object testKey = "test-key";
        ThulawaEvent mockEvent = createMockThulawaEvent();
        List<ThulawaEvent> batch = Arrays.asList(mockEvent);

        when(queueManager.getEarliestQueueKey()).thenReturn(testKey);

        // Access the microbatcher through reflection to mock it
        Field microbatcherField = ThulawaScheduler.class.getDeclaredField("microbatcher");
        microbatcherField.setAccessible(true);
        microbatcherField.set(scheduler, microBatcher);

        when(microBatcher.fetchBatch(testKey, 1)).thenReturn(batch);

        // When - invoke processBatch method through reflection
        java.lang.reflect.Method processBatchMethod = ThulawaScheduler.class.getDeclaredMethod("processBatch");
        processBatchMethod.setAccessible(true);
        processBatchMethod.invoke(scheduler);

        // Then
        verify(microBatcher).fetchBatch(testKey, 1);
        ArgumentCaptor<ThulawaTask> taskCaptor = ArgumentCaptor.forClass(ThulawaTask.class);
        verify(thulawaTaskManager).addActiveTask(eq(testKey), taskCaptor.capture());

        ThulawaTask capturedTask = taskCaptor.getValue();
        assertEquals(batch, capturedTask.getThulawaEvents());
    }

    @Test
    @DisplayName("Test process batch with micro batcher enabled")
    void testProcessBatchWithMicroBatcherEnabled() throws Exception {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        Object testKey = "test-key";
        ThulawaEvent mockEvent1 = createMockThulawaEvent();
        ThulawaEvent mockEvent2 = createMockThulawaEvent();
        List<ThulawaEvent> adaptiveBatch = Arrays.asList(mockEvent1, mockEvent2);

        when(queueManager.getEarliestQueueKey()).thenReturn(testKey);

        // Access the microbatcher through reflection to mock it
        Field microbatcherField = ThulawaScheduler.class.getDeclaredField("microbatcher");
        microbatcherField.setAccessible(true);
        microbatcherField.set(scheduler, microBatcher);

        when(microBatcher.fetchAdaptiveBatch(testKey)).thenReturn(adaptiveBatch);

        // When - invoke processBatch method through reflection
        java.lang.reflect.Method processBatchMethod = ThulawaScheduler.class.getDeclaredMethod("processBatch");
        processBatchMethod.setAccessible(true);
        processBatchMethod.invoke(scheduler);

        // Then
        verify(microBatcher).fetchAdaptiveBatch(testKey);
        ArgumentCaptor<ThulawaTask> taskCaptor = ArgumentCaptor.forClass(ThulawaTask.class);
        verify(thulawaTaskManager).addActiveTask(eq(testKey), taskCaptor.capture());

        ThulawaTask capturedTask = taskCaptor.getValue();
        assertEquals(adaptiveBatch, capturedTask.getThulawaEvents());
    }

    @Test
    @DisplayName("Test process batch with no queue key available")
    void testProcessBatchWithNoQueueKey() throws Exception {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        when(queueManager.getEarliestQueueKey()).thenReturn(null);

        // When - invoke processBatch method through reflection
        java.lang.reflect.Method processBatchMethod = ThulawaScheduler.class.getDeclaredMethod("processBatch");
        processBatchMethod.setAccessible(true);
        processBatchMethod.invoke(scheduler);

        // Then
        verify(queueManager).getEarliestQueueKey();
        verify(thulawaTaskManager, never()).addActiveTask(any(), any());
    }

    @Test
    @DisplayName("Test process batch with empty batch")
    void testProcessBatchWithEmptyBatch() throws Exception {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        Object testKey = "test-key";
        when(queueManager.getEarliestQueueKey()).thenReturn(testKey);

        // Access the microbatcher through reflection to mock it
        Field microbatcherField = ThulawaScheduler.class.getDeclaredField("microbatcher");
        microbatcherField.setAccessible(true);
        microbatcherField.set(scheduler, microBatcher);

        when(microBatcher.fetchAdaptiveBatch(testKey)).thenReturn(Collections.emptyList());

        // When - invoke processBatch method through reflection
        java.lang.reflect.Method processBatchMethod = ThulawaScheduler.class.getDeclaredMethod("processBatch");
        processBatchMethod.setAccessible(true);
        processBatchMethod.invoke(scheduler);

        // Then
        verify(microBatcher).fetchAdaptiveBatch(testKey);
        verify(thulawaTaskManager, never()).addActiveTask(any(), any());
    }

    @Test
    @DisplayName("Test start scheduling thread")
    void testStartSchedulingThread() {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // When
        scheduler.startSchedulingThread();

        // Then
        assertTrue(scheduler.isActive(), "Scheduler should be active after starting");
        verify(threadPoolRegistry).startDedicatedThread(
                eq("Thulawa-Scheduler-Thread"),
                any(Runnable.class)
        );
    }

    @Test
    @DisplayName("Test start scheduling thread when already running")
    void testStartSchedulingThreadWhenAlreadyRunning() {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        // First start
        scheduler.startSchedulingThread();

        // When - second start attempt
        scheduler.startSchedulingThread();

        // Then - should only start once
        verify(threadPoolRegistry, times(1)).startDedicatedThread(
                eq("Thulawa-Scheduler-Thread"),
                any(Runnable.class)
        );
    }

    @Test
    @DisplayName("Test scheduler with high priority keys")
    void testSchedulerWithHighPriorityKeys() {
        // Given
        Set<String> priorityKeys = new HashSet<>(Arrays.asList("urgent", "critical"));

        // When
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, priorityKeys, true
        );

        // Then
        assertNotNull(scheduler);
        // Note: High priority key handling logic would be tested in integration tests
        // as the current implementation doesn't show explicit priority handling in the scheduler
    }

    @Test
    @DisplayName("Test concurrent access to scheduler instance")
    void testConcurrentAccessToSchedulerInstance() throws InterruptedException {
        // Given
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<ThulawaScheduler> instances = Collections.synchronizedList(new ArrayList<>());

        // When - multiple threads try to get instance
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    ThulawaScheduler instance = ThulawaScheduler.getInstance(
                            queueManager, threadPoolRegistry, thulawaTaskManager,
                            thulawaMetrics, highPriorityKeySet, true
                    );
                    instances.add(instance);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        // Wait for all threads to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS), "All threads should complete within timeout");

        // Then - all instances should be the same
        assertEquals(threadCount, instances.size());
        ThulawaScheduler firstInstance = instances.get(0);
        for (ThulawaScheduler instance : instances) {
            assertSame(firstInstance, instance, "All instances should be the same singleton");
        }
    }

    // Helper method to create mock ThulawaEvent
    private ThulawaEvent createMockThulawaEvent() {
        Record<String, String> mockRecord = new Record<>("test-key", "test-value", System.currentTimeMillis());
        Runnable mockRunnable = () -> {
            // Mock processing logic
        };
        return new ThulawaEvent(mockRecord, System.currentTimeMillis(), mockRunnable);
    }

    // Additional integration-style test
    @Test
    @DisplayName("Test full scheduler workflow")
    void testFullSchedulerWorkflow() throws Exception {
        // Given
        scheduler = ThulawaScheduler.getInstance(
                queueManager, threadPoolRegistry, thulawaTaskManager,
                thulawaMetrics, highPriorityKeySet, true
        );

        Object testKey = "workflow-key";
        ThulawaEvent event1 = createMockThulawaEvent();
        ThulawaEvent event2 = createMockThulawaEvent();
        List<ThulawaEvent> batch = Arrays.asList(event1, event2);

        when(queueManager.getEarliestQueueKey()).thenReturn(testKey);

        // Access the microbatcher through reflection to mock it
        Field microbatcherField = ThulawaScheduler.class.getDeclaredField("microbatcher");
        microbatcherField.setAccessible(true);
        microbatcherField.set(scheduler, microBatcher);

        when(microBatcher.fetchAdaptiveBatch(testKey)).thenReturn(batch);

        // When
        scheduler.notifyScheduler(); // This should start the scheduler

        // Simulate one cycle of processing
        java.lang.reflect.Method processBatchMethod = ThulawaScheduler.class.getDeclaredMethod("processBatch");
        processBatchMethod.setAccessible(true);
        processBatchMethod.invoke(scheduler);

        // Then
        assertTrue(scheduler.isActive(), "Scheduler should be active");
        verify(threadPoolRegistry).startDedicatedThread(eq("Thulawa-Scheduler-Thread"), any(Runnable.class));
        verify(microBatcher).fetchAdaptiveBatch(testKey);
        verify(thulawaTaskManager).addActiveTask(eq(testKey), any(ThulawaTask.class));
    }
}