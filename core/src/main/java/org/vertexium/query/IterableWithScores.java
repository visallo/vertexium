package org.vertexium.query;

public interface IterableWithScores<T> extends Iterable<T> {
    Double getScore(Object id);
}
