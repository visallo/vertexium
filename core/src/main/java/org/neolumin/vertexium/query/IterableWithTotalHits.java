package org.neolumin.vertexium.query;

public interface IterableWithTotalHits<T> extends Iterable<T> {
    long getTotalHits();
}
