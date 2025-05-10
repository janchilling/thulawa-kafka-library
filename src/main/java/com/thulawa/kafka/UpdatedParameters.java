package com.thulawa.kafka;

import com.thulawa.kafka.internals.configs.ThulawaConfigs;
import com.thulawa.kafka.internals.configs.ThulawaStreamsConfig;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.Topology;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.thulawa.kafka.ThulawaKafkaStreams.THULAWA_METRICS_CONFIG;
import static com.thulawa.kafka.internals.metrics.ThulawaMetrics.THULAWA_METRICS_NAMESPACE;

public class UpdatedParameters {

    public final Topology topology;
    public final Properties props;
//    public final ThulawaClientSupplier clientSupplier;

    public static final Set<Object> highPriorityKeySet = new HashSet<>();

    public UpdatedParameters(Topology topology,
                             Properties props) {
        this.topology = topology;
        this.props = initializeThulawaMetrics(props);
//        this.clientSupplier = new ThulawaClientSupplier();
    }

    /**
     * Creates and integrates Thulawa-specific metrics into the provided properties.
     *
     * @param originalProps Original properties passed to the constructor.
     * @return A new Properties object with updated metrics configuration.
     */
    private Properties initializeThulawaMetrics(Properties originalProps) {
        Properties updatedProps = new Properties();
        updatedProps.putAll(originalProps);

        MetricConfig metricConfig = new MetricConfig();
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(ThulawaStreamsConfig.cerateThulawaStreamsConfig(originalProps).originals());

        ThulawaMetrics thulawaMetrics = new ThulawaMetrics(new Metrics(
                metricConfig,
                Collections.singletonList(jmxReporter),
                Time.SYSTEM,
                new KafkaMetricsContext(THULAWA_METRICS_NAMESPACE)
        ));

        updatedProps.put(THULAWA_METRICS_CONFIG, thulawaMetrics);

        String highPriorityKeyList = originalProps.getProperty(ThulawaConfigs.HIGH_PRIORITY_KEY_LIST);
        if (highPriorityKeyList != null && !highPriorityKeyList.isEmpty()) {
            String[] keys = highPriorityKeyList.split(",");
            for (String key : keys) {
                highPriorityKeySet.add(key.trim());
            }
        }
        updatedProps.put(ThulawaConfigs.HIGH_PRIORITY_KEY_MAP, highPriorityKeySet);

        updatedProps.put(ThulawaConfigs.PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED,
                originalProps.getOrDefault(ThulawaConfigs.PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED, ThulawaConfigs.DEFAULT_PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED));

        updatedProps.put(ThulawaConfigs.MICRO_BATCHER_ENABLED,
                originalProps.getOrDefault(ThulawaConfigs.MICRO_BATCHER_ENABLED, ThulawaConfigs.DEFAULT_MICRO_BATCHER_ENABLED));

        updatedProps.put(ThulawaConfigs.THULAWA_EXECUTOR_THREADPOOL_SIZE,
                originalProps.getOrDefault(ThulawaConfigs.THULAWA_EXECUTOR_THREADPOOL_SIZE, ThulawaConfigs.DEFAULT_EXECUTOR_THULAWA_THREADPOOL_SIZE));

        return updatedProps;
    }
}
