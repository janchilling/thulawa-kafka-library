//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.thulawa.kafka;

import com.thulawa.kafka.internals.suppliers.ThulawaClientSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ThulawaKafkaStreams extends KafkaStreams {
    private static final Logger LOG = LoggerFactory.getLogger(ThulawaKafkaStreams.class);
    private ThulawaClientSupplier thulawaClientSupplier;

    public ThulawaKafkaStreams(Topology topology, Properties props) {
        this(topology, props, Time.SYSTEM);
    }

//    public ThulawaKafkaStreams(Topology topology, Map<?, ?> configs, KafkaClientSupplier clientSupplier) {
//        this(topology, configs, clientSupplier, Time.SYSTEM);
//    }

    public ThulawaKafkaStreams(Topology topology, Properties props, Time time) {
        super(topology, props, new ThulawaClientSupplier(), time);
    }


}
