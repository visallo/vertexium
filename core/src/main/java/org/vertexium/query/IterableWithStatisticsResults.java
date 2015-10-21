package org.vertexium.query;

@Deprecated
public interface IterableWithStatisticsResults<T> extends Iterable<T> {
    @Deprecated
    StatisticsResult getStatisticsResults(String name);
}
