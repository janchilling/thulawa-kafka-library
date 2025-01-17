package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JVMMetricsRecorder implements AutoCloseable {
    private static final String METRIC_GROUP_NAME = "thulawa-jvm-metrics";

    private final ThulawaMetrics metrics;
    private final Sensor heapMemorySensor;
    private final Sensor cpuUsageSensor;
    private final MetricName heapMemoryUsedMetric;
    private final MetricName heapMemoryMaxMetric;
    private final MetricName cpuUsageMetric;

    private final MemoryMXBean memoryMXBean;
    private final OperatingSystemMXBean osMXBean;

    private final ScheduledExecutorService scheduler;

    public JVMMetricsRecorder(ThulawaMetrics metrics) {
        this.metrics = metrics;
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.osMXBean = ManagementFactory.getOperatingSystemMXBean();

        // Scheduler for periodic recording
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        // Register Heap Memory Metrics
        this.heapMemorySensor = metrics.addSensor("heap-memory-sensor");
        this.heapMemoryUsedMetric = metrics.createMetricName(
                "heap-memory-used",
                METRIC_GROUP_NAME,
                "The amount of heap memory currently used."
        );
        this.heapMemoryMaxMetric = metrics.createMetricName(
                "heap-memory-max",
                METRIC_GROUP_NAME,
                "The maximum amount of heap memory available."
        );
        this.heapMemorySensor.add(heapMemoryUsedMetric, new CumulativeSum());
        this.heapMemorySensor.add(heapMemoryMaxMetric, new Max());

        // Register CPU Usage Metrics
        this.cpuUsageSensor = metrics.addSensor("cpu-usage-sensor");
        this.cpuUsageMetric = metrics.createMetricName(
                "cpu-usage",
                METRIC_GROUP_NAME,
                "The system-wide CPU usage as a percentage."
        );
        this.cpuUsageSensor.add(cpuUsageMetric, new Avg());

        // Register Measurable Metrics
        registerMeasurableMetrics();

        // Schedule the periodic recording of metrics every 5 seconds
        scheduleMetricRecording();
    }

    private void registerMeasurableMetrics() {
        // Heap memory used (measurable)
        metrics.addMetric(heapMemoryUsedMetric, (Measurable) (config, now) ->
                (double) memoryMXBean.getHeapMemoryUsage().getUsed());

        // Heap memory max (measurable)
        metrics.addMetric(heapMemoryMaxMetric, (Measurable) (config, now) ->
                (double) memoryMXBean.getHeapMemoryUsage().getMax());

        // CPU usage (measurable)
        if (osMXBean instanceof com.sun.management.OperatingSystemMXBean) {
            metrics.addMetric(cpuUsageMetric, (Measurable) (config, now) ->
                    ((com.sun.management.OperatingSystemMXBean) osMXBean).getSystemCpuLoad() * 100);
        }
    }

    private void scheduleMetricRecording() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                recordMetrics();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.SECONDS); // Schedule every 5 seconds
    }

    public void recordMetrics() {
        long usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
        long maxHeapMemory = memoryMXBean.getHeapMemoryUsage().getMax();
        double cpuLoad = osMXBean instanceof com.sun.management.OperatingSystemMXBean
                ? ((com.sun.management.OperatingSystemMXBean) osMXBean).getSystemCpuLoad() * 100
                : 0.0;

        // Record metrics to sensors
        heapMemorySensor.record(usedHeapMemory);
        heapMemorySensor.record(maxHeapMemory);
        cpuUsageSensor.record(cpuLoad);
    }

    @Override
    public void close() {
        // Shut down the scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }

        // Remove sensors and metrics
        metrics.removeSensor(heapMemorySensor.name());
        metrics.removeSensor(cpuUsageSensor.name());
        metrics.removeMetric(heapMemoryUsedMetric);
        metrics.removeMetric(heapMemoryMaxMetric);
        metrics.removeMetric(cpuUsageMetric);
    }
}
