package com.thulawa.kafka;

import com.thulawa.kafka.internals.configs.ThulawaConfigs;
import com.thulawa.kafka.internals.helpers.QueueManager;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.processor.ThulawaProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class ThulawaProcessorTest {

    private ThulawaProcessor<String, String, String, String> thulawaProcessor;

    private Processor<String, String, String, String> mockDelegateProcessor;
    private ProcessorContext<String, String> mockContext;
    private ThulawaMetrics mockMetrics;
    private QueueManager mockQueueManager;

    private final Set<String> highPriorityKeys = new HashSet<>(Set.of("high1", "high2"));

    @BeforeEach
    void setup() {
        mockDelegateProcessor = mock(Processor.class);
        mockContext = mock(ProcessorContext.class);
        mockMetrics = mock(ThulawaMetrics.class);
        mockQueueManager = mock(QueueManager.class);

        // Mocks for context.appConfigs()
        Map<String, Object> appConfigs = new HashMap<>();
        appConfigs.put(ThulawaConfigs.THULAWA_EXECUTOR_THREADPOOL_SIZE, 1);
        appConfigs.put(ThulawaConfigs.HIGH_PRIORITY_KEY_MAP, highPriorityKeys);
        appConfigs.put("THULAWA_METRICS_CONFIG", mockMetrics);

        when(mockContext.appConfigs()).thenReturn(appConfigs);
        when(mockContext.currentSystemTimeMs()).thenReturn(123456789L);

        // Replace singleton instances with mocks if needed
        mockStatic(QueueManager.class).when(() -> QueueManager.getInstance(any())).thenReturn(mockQueueManager);

        thulawaProcessor = new ThulawaProcessor<>(mockDelegateProcessor);
    }

    @Test
    void testInitInitializesAllComponents() {
        assertDoesNotThrow(() -> thulawaProcessor.init(mockContext));
    }

    @Test
    void testProcessAddsToQueue() {
        thulawaProcessor.init(mockContext);

        Record<String, String> record = new Record<>("test-key", "test-value", 0L);

        thulawaProcessor.process(record);

        ArgumentCaptor<ThulawaEvent> captor = ArgumentCaptor.forClass(ThulawaEvent.class);

        verify(mockQueueManager, times(1)).addToKeyBasedQueue(eq("test-key"), captor.capture());

        ThulawaEvent event = captor.getValue();
        assertNotNull(event);
        assertEquals("test-value", ((Record<?, ?>) event.getRecordValue()));
    }

    @Test
    void testCloseLogsWithoutException() {
        assertDoesNotThrow(() -> thulawaProcessor.close());
    }
}
