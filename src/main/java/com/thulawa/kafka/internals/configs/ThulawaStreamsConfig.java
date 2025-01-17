package com.thulawa.kafka.internals.configs;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ThulawaStreamsConfig extends StreamsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ThulawaStreamsConfig.class);

    public static ThulawaStreamsConfig cerateThulawaStreamsConfig(final Map<?, ?> props) {
        return new ThulawaStreamsConfig(props);
    }

    private ThulawaStreamsConfig(Map<?, ?> props) {
        super(props);
    }

}
