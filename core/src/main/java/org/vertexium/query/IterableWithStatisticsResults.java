package org.vertexium.query;

public interface IterableWithStatisticsResults<T> extends Iterable<T> {
    StatisticsResult getStatisticsResults(String name);
}
