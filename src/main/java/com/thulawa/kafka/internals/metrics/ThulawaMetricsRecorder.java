package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.LinkedHashMap;

// Have to properly update the metrics exposing with a proper design pattern
// This is temporary, and will be updated in the later enhancements
public class ThulawaMetricsRecorder {

    public static final String GROUP_NAME = "thulawa-metrics";

    public static final String KEY_BASED_QUEUES_SIZE = "key-based-queues-size";
    public static final String KEY_BASED_QUEUES_SIZE_DESC = "Size of the Key Based Queues created and modified dynamically";

    private final ThulawaMetrics metrics;
    private final Sensor keyBasedQueuesSensor;
    private final LinkedHashMap<String, String> tag = new LinkedHashMap<>();

    public ThulawaMetricsRecorder(ThulawaMetrics metrics) {
        this.metrics = metrics;

        this.tag.put("Hello", "Hello");

        keyBasedQueuesSensor = metrics.addSensor(KEY_BASED_QUEUES_SIZE);
        keyBasedQueuesSensor.add(
                new MetricName(KEY_BASED_QUEUES_SIZE, KEY_BASED_QUEUES_SIZE_DESC, GROUP_NAME, tag ),
                new Value()
        );
    }

    public void updateKeyBasedQueueSizes(double size){
        this.keyBasedQueuesSensor.record(size);
    }

}
