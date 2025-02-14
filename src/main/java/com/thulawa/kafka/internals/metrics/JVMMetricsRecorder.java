package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Sensor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JVMMetricsRecorder implements AutoCloseable {

    private static final String METRIC_GROUP_NAME = "thulawa-jvm-metrics";

    private final ThulawaMetrics metrics;
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

        // Register Heap Memory Used Metric (Gauge)
        this.heapMemoryUsedMetric = metrics.createMetricName(
                "thulawa-heap-memory-used",
                METRIC_GROUP_NAME,
                "The amount of heap memory currently used."
        );
        if (!metrics.doesMetricExist(heapMemoryUsedMetric)) {
            metrics.addMetric(heapMemoryUsedMetric, (Measurable) (config, now) ->
                    (double) memoryMXBean.getHeapMemoryUsage().getUsed());
        }

        // Register Heap Memory Max Metric (Gauge)
        this.heapMemoryMaxMetric = metrics.createMetricName(
                "thulawa-heap-memory-max",
                METRIC_GROUP_NAME,
                "The maximum amount of heap memory available."
        );
        if (!metrics.doesMetricExist(heapMemoryMaxMetric)) {
            metrics.addMetric(heapMemoryMaxMetric, (Measurable) (config, now) ->
                    (double) memoryMXBean.getHeapMemoryUsage().getMax());
        }

        // Register CPU Usage Metric (Gauge)
        this.cpuUsageMetric = metrics.createMetricName(
                "thulawa-cpu-usage",
                METRIC_GROUP_NAME,
                "The system-wide CPU usage as a percentage."
        );
        if (!metrics.doesMetricExist(cpuUsageMetric)) {
            if (osMXBean instanceof com.sun.management.OperatingSystemMXBean) {
                metrics.addMetric(cpuUsageMetric, (Measurable) (config, now) ->
                        ((com.sun.management.OperatingSystemMXBean) osMXBean).getSystemCpuLoad() * 100);
            }
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        // This is not necessary will remove in the future once the testing from Grafana and Scheduler is completed
//        scheduleMetricValidation();
    }
    // This is not necessary will remove in the future once the testing from Grafana and Scheduler is completed
    private void scheduleMetricValidation() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                validateMetrics();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    // This is not necessary will remove in the future once the testing from Grafana and Scheduler is completed
    private void validateMetrics() {
        // These validations are optional and can be removed
        long usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
        long maxHeapMemory = memoryMXBean.getHeapMemoryUsage().getMax();
        double cpuLoad = osMXBean instanceof com.sun.management.OperatingSystemMXBean
                ? ((com.sun.management.OperatingSystemMXBean) osMXBean).getSystemCpuLoad() * 100
                : 0.0;

        System.out.printf("Heap Used: %d, Heap Max: %d, CPU Usage: %.2f%%%n",
                usedHeapMemory, maxHeapMemory, cpuLoad);
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

        // Remove metrics
        metrics.removeMetric(heapMemoryUsedMetric);
        metrics.removeMetric(heapMemoryMaxMetric);
        metrics.removeMetric(cpuUsageMetric);
    }
}
