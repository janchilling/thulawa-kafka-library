package com.thulawa.kafka;

import com.thulawa.kafka.MicroBatcher.MicroBatcher;
import com.thulawa.kafka.internals.helpers.ThreadPoolRegistry;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ThulawaTaskManagerTest {

    private ThulawaTaskManager taskManager;
    private ThreadPoolRegistry threadPoolRegistry;
    private ThulawaMetrics thulawaMetrics;
    private MicroBatcher microBatcher;
    private ThulawaMetricsRecorder thulawaMetricsRecorder;
    private long mockTimestamp;

    @BeforeEach
    void setUp() {
        threadPoolRegistry = mock(ThreadPoolRegistry.class);
        thulawaMetrics = mock(ThulawaMetrics.class);
        microBatcher = mock(MicroBatcher.class);
        thulawaMetricsRecorder = mock(ThulawaMetricsRecorder.class);
        mockTimestamp = System.currentTimeMillis();

        taskManager = new ThulawaTaskManager(
                threadPoolRegistry,
                thulawaMetrics,
                microBatcher,
                thulawaMetricsRecorder,
                false // virtual thread mode
        );

        // Simulate a thread pool launching a thread that runs the task's run() method
        doAnswer(invocation -> {
            String threadName = invocation.getArgument(0);
            Runnable runnable = invocation.getArgument(1);
            Thread t = new Thread(runnable, threadName);
            t.start();
            return null;
        }).when(threadPoolRegistry).startDedicatedThread(anyString(), any(Runnable.class));
    }

    @Test
    void testAddAndProcessTask() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        // Create a mock runnable that counts down the latch when executed
        Runnable mockRunnable = mock(Runnable.class);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockRunnable).run();

        Record<String, String> kafkaRecord = new Record<>(
                "sample-key",
                "sample-value",
                mockTimestamp,
                new RecordHeaders()
        );

        ThulawaEvent event = new ThulawaEvent(kafkaRecord, mockTimestamp, mockRunnable);
        ThulawaTask task = new ThulawaTask("Thulawa-Executor-Thread-Pool", List.of(event));
        taskManager.addActiveTask("sample-key", task);

        // Wait up to 1 second for the task to run
        boolean ran = latch.await(1, TimeUnit.SECONDS);
        assertEquals(true, ran, "Expected mockRunnable.run() to be called.");

        verify(mockRunnable, times(1)).run();
        assertEquals(1, taskManager.getSuccessCount("sample-key"));
        assertEquals(1, taskManager.getTotalSuccessCount());
        verify(thulawaMetricsRecorder, atLeastOnce()).updateTotalProcessedCount(anyLong());
    }
}
