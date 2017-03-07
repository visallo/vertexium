package org.vertexium.query;

import java.util.Map;

public interface IterableWithScores<T> extends Iterable<T> {
    Map<Object, Double> getScores();
}
