package com.thulawa.kafka.internals.configs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ThulawaConfigs extends AbstractConfig {

    public static final String HIGH_PRIORITY_KEY_LIST = "high.priority.key.list";
    public static final String HIGH_PRIORITY_KEY_LIST_DESCRIPTION = "Comma separated High Priority Keys";

    public static final String HIGH_PRIORITY_KEY_MAP = "high.priority.key.map";
    public static final String HIGH_PRIORITY_KEY_MAP_DESCRIPTION = "Map of the High Priority Keys";

    private static final ConfigDef definition = new ConfigDef()
            .define(
                    HIGH_PRIORITY_KEY_LIST,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    HIGH_PRIORITY_KEY_LIST_DESCRIPTION
            )
            .define(
                    HIGH_PRIORITY_KEY_MAP,
                    ConfigDef.Type.CLASS,
                    ConfigDef.Importance.HIGH,
                    HIGH_PRIORITY_KEY_MAP_DESCRIPTION
            );

    public ThulawaConfigs(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }
}
