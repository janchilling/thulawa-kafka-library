//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.thulawa.kafka;

import com.thulawa.kafka.internals.configs.ThulawaStreamsConfig;
import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.suppliers.ThulawaClientSupplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import static com.thulawa.kafka.internals.metrics.ThulawaMetrics.THULAWA_METRICS_NAMESPACE;

public class ThulawaKafkaStreams extends KafkaStreams {

    private static final Logger LOG = LoggerFactory.getLogger(ThulawaKafkaStreams.class);

    public static final String THULAWA_METRICS_CONFIG = "__thulawa.metrics.config__";

    private ThulawaClientSupplier thulawaClientSupplier;
    private final ThulawaMetrics thulawaMetrics;

    public ThulawaKafkaStreams(Topology topology, Properties props) {
        this( new UpdatedParameters(topology, props) );
    }

    public ThulawaKafkaStreams( UpdatedParameters updatedParameters ) {
        super(updatedParameters.topology, updatedParameters.props, updatedParameters.clientSupplier, Time.SYSTEM);

        // Have to properly update the metrics exposing with a proper design pattern
        // This is temporary, and will be updated in the later enhancements
        this.thulawaMetrics = (ThulawaMetrics) updatedParameters.props.get(THULAWA_METRICS_CONFIG);
    }

}
