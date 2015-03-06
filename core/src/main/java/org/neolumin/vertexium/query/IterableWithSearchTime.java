package org.neolumin.vertexium.query;

public interface IterableWithSearchTime<T> extends Iterable<T> {
    long getSearchTimeNanoSeconds();
}
