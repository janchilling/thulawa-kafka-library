package com.thulawa.kafka.internals.configs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ThulawaConfigs extends AbstractConfig {

    public static final String HIGH_PRIORITY_KEY_LIST = "high.priority.key.list";
    public static final String HIGH_PRIORITY_KEY_LIST_DESCRIPTION = "Comma separated High Priority Keys";

    public static final String HIGH_PRIORITY_KEY_MAP = "high.priority.key.map";
    public static final String HIGH_PRIORITY_KEY_MAP_DESCRIPTION = "Map of the High Priority Keys";

    public static final boolean DEFAULT_PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED = false;
    public static final String PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED = "prioritized.adaptive.scheduler.enabled";
    public static final String PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED_DESCRIPTION = "Priority key based Adaptive Scheduler";

    public static final boolean DEFAULT_MICRO_BATCHER_ENABLED = false;
    public static final String MICRO_BATCHER_ENABLED = "micro.batcher.enabled";
    public static final String MICRO_BATCHER_ENABLED_DESCRIPTION = "Micro Batcher for submitting tasks to threads";

    public static final int DEFAULT_EXECUTOR_THULAWA_THREADPOOL_SIZE = 0;
    public static final String THULAWA_EXECUTOR_THREADPOOL_SIZE = "thulawa.threadpool.size";
    public static final String THULAWA_EXECUTOR_THREADPOOL_SIZE_DESCRIPTION = "Size of the Thulawa Threadpool";

    private static final ConfigDef definition = new ConfigDef()
            .define(
                    HIGH_PRIORITY_KEY_LIST,
                    ConfigDef.Type.CLASS,
                    ConfigDef.Importance.HIGH,
                    HIGH_PRIORITY_KEY_LIST_DESCRIPTION
            )
            .define(
                    HIGH_PRIORITY_KEY_MAP,
                    ConfigDef.Type.CLASS,
                    ConfigDef.Importance.HIGH,
                    HIGH_PRIORITY_KEY_MAP_DESCRIPTION
            ).define(
                    PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    ConfigDef.Importance.HIGH,
                    PRIORITIZED_ADAPTIVE_SCHEDULER_ENABLED_DESCRIPTION
            ).define(
                    MICRO_BATCHER_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    ConfigDef.Importance.HIGH,
                    MICRO_BATCHER_ENABLED_DESCRIPTION
            ).define(
                    THULAWA_EXECUTOR_THREADPOOL_SIZE,
                    ConfigDef.Type.INT,
                    ConfigDef.Importance.HIGH,
                    THULAWA_EXECUTOR_THREADPOOL_SIZE_DESCRIPTION
            );

    public ThulawaConfigs(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }
}
