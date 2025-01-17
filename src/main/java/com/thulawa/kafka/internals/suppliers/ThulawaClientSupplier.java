package com.thulawa.kafka.internals.suppliers;

import com.thulawa.kafka.internals.clients.ThulawaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import java.util.Map;

public class ThulawaClientSupplier extends DefaultKafkaClientSupplier {

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        return new ThulawaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

}
