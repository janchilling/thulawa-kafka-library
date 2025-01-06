//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.thulawa.kafka;

import com.thulawa.kafka.internals.metrics.ThulawaMetrics;
import com.thulawa.kafka.internals.metrics.ThulawaMetricsRecorder;
import com.thulawa.kafka.internals.suppliers.ThulawaClientSupplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ThulawaKafkaStreams extends KafkaStreams {
    private static final Logger LOG = LoggerFactory.getLogger(ThulawaKafkaStreams.class);
    private ThulawaClientSupplier thulawaClientSupplier;
    private final ThulawaMetrics thulawaMetrics;
    private final ThulawaMetricsRecorder thulawaMetricsRecorder;

    public ThulawaKafkaStreams(Topology topology, Properties props) {
        this(topology, props, Time.SYSTEM);
    }

//    public ThulawaKafkaStreams(Topology topology, Map<?, ?> configs, KafkaClientSupplier clientSupplier) {
//        this(topology, configs, clientSupplier, Time.SYSTEM);
//    }

    public ThulawaKafkaStreams(Topology topology, Properties props, Time time) {
        super(topology, props, new ThulawaClientSupplier(), time);

        // Have to properly update the metrics exposing with a proper design pattern
        // This is temporary, and will be updated in the later enhancements
        this.thulawaMetrics = new ThulawaMetrics(new Metrics());
        this.thulawaMetricsRecorder = new ThulawaMetricsRecorder(this.thulawaMetrics);
    }


}
