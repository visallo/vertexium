package org.vertexium.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;

public class BatchCollector<T> implements Collector<T, List<T>, List<T>> {
    private final int batchSize;
    private final Consumer<List<T>> batchProcessor;

    public BatchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        this.batchSize = batchSize;
        this.batchProcessor = batchProcessor;
    }

    @Override
    public Supplier<List<T>> supplier() {
        return () -> new ArrayList<>(batchSize);
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return (ts, t) -> {
            ts.add(t);
            if (ts.size() >= batchSize) {
                batchProcessor.accept(ts);
                ts.clear();
            }
        };
    }

    @Override
    public BinaryOperator<List<T>> combiner() {
        return (ts, ots) -> {
            if (ts.size() > 0) {
                batchProcessor.accept(ts);
                ts.clear();
            }
            if (ots.size() > 0) {
                batchProcessor.accept(ots);
                ots.clear();
            }
            return Collections.emptyList();
        };
    }

    @Override
    public Function<List<T>, List<T>> finisher() {
        return ts -> {
            batchProcessor.accept(ts);
            ts.clear();
            return Collections.emptyList();
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
