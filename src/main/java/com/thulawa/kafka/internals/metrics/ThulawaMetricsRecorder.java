package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.Rate;

import java.util.LinkedHashMap;

/**
 * ThulawaMetricsRecorder exposes various metrics related to task processing.
 */
public class ThulawaMetricsRecorder {

    public static final String GROUP_NAME = "thulawa-metrics";

    private static final String HIGH_PRIORITY_TASKS_PROCESSED = "thulawa-high-priority-tasks-processed";
    private static final String HIGH_PRIORITY_TASKS_DESC = "Number of tasks processed by high-priority threads";

    private static final String LOW_PRIORITY_TASKS_PROCESSED = "thulawa-low-priority-tasks-processed";
    private static final String LOW_PRIORITY_TASKS_DESC = "Number of tasks processed by low-priority threads";

    private static final String COMBINED_THROUGHPUT = "thulawa-combined-throughput";
    private static final String COMBINED_THROUGHPUT_DESC = "Combined throughput of tasks processed per second";

    private final ThulawaMetrics metrics;

    private final Sensor highPrioritySensor;
    private final Sensor lowPrioritySensor;
    private final Sensor combinedThroughputSensor;

    private final LinkedHashMap<String, String> tag = new LinkedHashMap<>();

    public ThulawaMetricsRecorder(ThulawaMetrics metrics) {
        this.metrics = metrics;

        // Example tag setup (expandable if needed)
        this.tag.put("application", "Thulawa");

        // Sensor for high-priority tasks
        highPrioritySensor = metrics.addSensor(HIGH_PRIORITY_TASKS_PROCESSED);
        highPrioritySensor.add(
                metrics.createMetricName(HIGH_PRIORITY_TASKS_PROCESSED, GROUP_NAME, HIGH_PRIORITY_TASKS_DESC),
                new Value()
        );
        highPrioritySensor.add(
                metrics.createMetricName(HIGH_PRIORITY_TASKS_PROCESSED + "-rate", GROUP_NAME, HIGH_PRIORITY_TASKS_DESC + " rate"),
                new Rate()
        );

        // Sensor for low-priority tasks
        lowPrioritySensor = metrics.addSensor(LOW_PRIORITY_TASKS_PROCESSED);
        lowPrioritySensor.add(
                metrics.createMetricName(LOW_PRIORITY_TASKS_PROCESSED, GROUP_NAME, LOW_PRIORITY_TASKS_DESC),
                new Value()
        );
        lowPrioritySensor.add(
                metrics.createMetricName(LOW_PRIORITY_TASKS_PROCESSED + "-rate", GROUP_NAME, LOW_PRIORITY_TASKS_DESC + " rate"),
                new Rate()
        );

        // Sensor for combined throughput
        combinedThroughputSensor = metrics.addSensor(COMBINED_THROUGHPUT);
        combinedThroughputSensor.add(
                metrics.createMetricName(COMBINED_THROUGHPUT, GROUP_NAME, COMBINED_THROUGHPUT_DESC),
                new Rate()
        );
    }

    /**
     * Updates the high-priority task count.
     * @param count Number of tasks processed.
     */
    public void updateHighPriorityTasks(double count) {
        highPrioritySensor.record(count);
        combinedThroughputSensor.record(count); // Update combined throughput
    }

    /**
     * Updates the low-priority task count.
     * @param count Number of tasks processed.
     */
    public void updateLowPriorityTasks(double count) {
        lowPrioritySensor.record(count);
        combinedThroughputSensor.record(count); // Update combined throughput
    }
}
