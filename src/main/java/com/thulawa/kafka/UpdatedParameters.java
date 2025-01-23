package com.thulawa.kafka;

import com.thulawa.kafka.internals.configs.ThulawaConfigs;
import com.thulawa.kafka.internals.configs.ThulawaStreamsConfig;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.suppliers.ThulawaClientSupplier;
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
    public final ThulawaClientSupplier clientSupplier;

    public static final Set<String> highPriorityKeySet = new HashSet<>();

    public UpdatedParameters(Topology topology, Properties props) {
        this.topology = topology;
        this.props = initializeThulawaMetrics(props);
        this.clientSupplier = new ThulawaClientSupplier();
    }

    /**
     * Creates and integrates Thulawa-specific metrics into the provided properties.
     *
     * @param originalProps Original properties passed to the constructor.
     * @return A new Properties object with updated metrics configuration.
     */
    private Properties initializeThulawaMetrics(Properties originalProps) {
        // Create a copy of the original properties to avoid mutation
        Properties updatedProps = new Properties();
        updatedProps.putAll(originalProps);

        // Configure the Kafka Metrics
        MetricConfig metricConfig = new MetricConfig();
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(ThulawaStreamsConfig.cerateThulawaStreamsConfig(originalProps).originals());

        // Create ThulawaMetrics with a custom Metrics object
        ThulawaMetrics thulawaMetrics = new ThulawaMetrics(new Metrics(
                metricConfig,
                Collections.singletonList(jmxReporter),
                Time.SYSTEM,
                new KafkaMetricsContext(THULAWA_METRICS_NAMESPACE)
        ));

        // Add the ThulawaMetrics instance to the properties
        updatedProps.put(THULAWA_METRICS_CONFIG, thulawaMetrics);

        //write the code to get the keys seperated from commas and add to the map
        String highPriorityKeyList = originalProps.getProperty(ThulawaConfigs.HIGH_PRIORITY_KEY_LIST);
        if (highPriorityKeyList != null && !highPriorityKeyList.isEmpty()) {
            // Split the comma-separated string and add the keys to the set
            String[] keys = highPriorityKeyList.split(",");
            for (String key : keys) {
                highPriorityKeySet.add(key.trim()); // Trim to avoid leading/trailing spaces
            }
        }

        updatedProps.put(ThulawaConfigs.HIGH_PRIORITY_KEY_MAP, highPriorityKeySet);
        return updatedProps;
    }
}
