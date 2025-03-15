package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.LinkedHashMap;

/**
 * ThulawaMetricsRecorder exposes various metrics related to task processing.
 */
public class ThulawaMetricsRecorder {

    public static final String GROUP_NAME = "thulawa-metrics";

    private static final String HIGH_PRIORITY_TASKS_PROCESSED = "thulawa-high-priority-tasks-processed";
    private static final String HIGH_PRIORITY_TASKS_DESC = "Total number of high-priority tasks processed";

    private static final String LOW_PRIORITY_TASKS_PROCESSED = "thulawa-low-priority-tasks-processed";
    private static final String LOW_PRIORITY_TASKS_DESC = "Total number of low-priority tasks processed";

    private static final String COMBINED_THROUGHPUT = "thulawa-combined-throughput";
    private static final String COMBINED_THROUGHPUT_DESC = "Combined throughput of tasks processed per second";

    private static final String COMBINED_TASKS_PROCESSED = "thulawa-combined-tasks-processed";
    private static final String COMBINED_TASKS_DESC = "Total number of all tasks processed";

    private static final String TOTAL_EVENTS_PROCESSED = "thulawa-total-number-of-events-processed";
    private static final String TOTAL_EVENTS_PROCESSED_DESC = "Total number of all events processed";

    private static final String TOTAL_EVENTS_PROCESSED_RATE = "thulawa-event-processing-rate";
    private static final String TOTAL_EVENTS_PROCESSED_RATE_DESC = "Rate of successfully processing Thulawa events";

    private final ThulawaMetrics metrics;
    private final Sensor highPrioritySensor;
    private final Sensor lowPrioritySensor;
    private final Sensor combinedThroughputSensor;
    private final Sensor combinedTasksSensor;
    private final Sensor totalEventsProcessedSensor;

    private final LinkedHashMap<String, String> tag = new LinkedHashMap<>();

    public ThulawaMetricsRecorder(ThulawaMetrics metrics) {
        this.metrics = metrics;
        this.tag.put("application", "Thulawa");

        // Sensor for high-priority tasks
        highPrioritySensor = metrics.addSensor(HIGH_PRIORITY_TASKS_PROCESSED);
        highPrioritySensor.add(
                metrics.createMetricName(HIGH_PRIORITY_TASKS_PROCESSED, GROUP_NAME, HIGH_PRIORITY_TASKS_DESC),
                new CumulativeSum()
        );
        highPrioritySensor.add(
                metrics.createMetricName(HIGH_PRIORITY_TASKS_PROCESSED + "-rate", GROUP_NAME, HIGH_PRIORITY_TASKS_DESC + " rate"),
                new Rate()
        );

        // Sensor for low-priority tasks
        lowPrioritySensor = metrics.addSensor(LOW_PRIORITY_TASKS_PROCESSED);
        lowPrioritySensor.add(
                metrics.createMetricName(LOW_PRIORITY_TASKS_PROCESSED, GROUP_NAME, LOW_PRIORITY_TASKS_DESC),
                new CumulativeSum()
        );
        lowPrioritySensor.add(
                metrics.createMetricName(LOW_PRIORITY_TASKS_PROCESSED + "-rate", GROUP_NAME, LOW_PRIORITY_TASKS_DESC + " rate"),
                new Rate()
        );

        // Sensor for combined throughput (Rate)
        combinedThroughputSensor = metrics.addSensor(COMBINED_THROUGHPUT);
        combinedThroughputSensor.add(
                metrics.createMetricName(COMBINED_THROUGHPUT, GROUP_NAME, COMBINED_THROUGHPUT_DESC),
                new Rate()
        );

        // Sensor for total combined tasks processed (Cumulative)
        combinedTasksSensor = metrics.addSensor(COMBINED_TASKS_PROCESSED);
        combinedTasksSensor.add(
                metrics.createMetricName(COMBINED_TASKS_PROCESSED, GROUP_NAME, COMBINED_TASKS_DESC),
                new CumulativeSum()
        );

        totalEventsProcessedSensor = metrics.addSensor(TOTAL_EVENTS_PROCESSED);
        totalEventsProcessedSensor.add(
                metrics.createMetricName(TOTAL_EVENTS_PROCESSED, GROUP_NAME, TOTAL_EVENTS_PROCESSED_DESC),
                new Value()
        );
        totalEventsProcessedSensor.add(
                metrics.createMetricName(TOTAL_EVENTS_PROCESSED_RATE, GROUP_NAME, TOTAL_EVENTS_PROCESSED_RATE_DESC),
                new Rate()
        );
    }

    /**
     * Updates the high-priority task count.
     * @param count Number of tasks processed.
     */
    public void updateHighPriorityTasks(double count) {
        highPrioritySensor.record(count);
        combinedThroughputSensor.record(count);
        combinedTasksSensor.record(count);
    }

    /**
     * Updates the low-priority task count.
     * @param count Number of tasks processed.
     */
    public void updateLowPriorityTasks(double count) {
        lowPrioritySensor.record(count);
        combinedThroughputSensor.record(count);
        combinedTasksSensor.record(count);
    }

    public void updateTotalProcessedCount(long count){
        totalEventsProcessedSensor.record(count);
    }
}
