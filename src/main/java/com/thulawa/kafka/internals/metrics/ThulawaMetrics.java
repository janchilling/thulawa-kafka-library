package com.thulawa.kafka.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class ThulawaMetrics implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ThulawaMetrics.class);

    public static final String THULAWA_METRICS_NAMESPACE = "com.thulawa";
    private final Metrics metrics;

    public ThulawaMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public void addMetric(final MetricName metricName, final Measurable measurable) {
        metrics.addMetric(metricName, measurable);
    }

    public MetricName createMetricName(String name, String group, String description) {
        return metrics.metricName(name, group, description);
    }

    public Sensor addSensor(final String sensorName) {
        return metrics.sensor(sensorName);
    }

    public Sensor getSensor(final String sensorName) {
        return metrics.getSensor(sensorName);
    }

    public void removeSensor(final String sensorName) {
        metrics.removeSensor(sensorName);
    }

    public void removeMetric(final MetricName metricName) {
        metrics.removeMetric(metricName);
    }

    public Map<MetricName, KafkaMetric> metrics() {
        return metrics.metrics();
    }

    public boolean doesMetricExist(MetricName metricname){
        return this.metrics.metrics().containsKey(metricname);
    }


    @Override
    public void close() throws IOException {
        this.metrics.close();
    }
}
