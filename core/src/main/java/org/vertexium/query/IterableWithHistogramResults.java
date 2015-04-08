package org.vertexium.query;

public interface IterableWithHistogramResults<T> extends Iterable<T> {
    HistogramResult getHistogramResults(String name);
}
