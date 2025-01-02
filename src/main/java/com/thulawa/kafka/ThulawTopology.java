package com.thulawa.kafka;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class ThulawTopology extends Topology {

    public InternalTopologyBuilder getInternalTopologyBuilder(){
        return super.internalTopologyBuilder;
    }
}
