package org.vertexium.query;

@Deprecated
public interface IterableWithHistogramResults<T> extends Iterable<T> {
    @Deprecated
    HistogramResult getHistogramResults(String name);
}
