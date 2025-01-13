package com.thulawa.kafka.internals.suppliers;

import com.thulawa.kafka.internals.processor.ThulawaProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class ThulawaProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut>{

    private final Processor processor;

    public static <KIn, VIn, KOut, VOut> ThulawaProcessorSupplier<KIn, VIn, KOut, VOut> createThulawaProcessorSupplier(Processor processor) {
        return new ThulawaProcessorSupplier(processor);
    }

    private ThulawaProcessorSupplier(Processor processor) {
        this.processor = processor;
    }

    public ThulawaProcessor<KIn, VIn, KOut, VOut> get() {
        return new ThulawaProcessor<>(processor);
    }

}
