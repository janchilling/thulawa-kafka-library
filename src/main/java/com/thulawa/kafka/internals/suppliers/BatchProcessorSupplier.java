package com.thulawa.kafka.internals.suppliers;

import com.thulawa.kafka.internals.processor.BatchProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class BatchProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut>{


    public static <KIn, VIn, KOut, VOut> BatchProcessorSupplier<KIn, VIn, KOut, VOut> createAsyncProcessorSupplier() {
        return new BatchProcessorSupplier();
    }

    private BatchProcessorSupplier() {

    }

    public BatchProcessor<KIn, VIn, KOut, VOut> get() {
        return new BatchProcessor<>();
    }

}
