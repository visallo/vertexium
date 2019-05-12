package org.vertexium.search;

import org.vertexium.query.AggregationResult;

import java.util.stream.Stream;

public class EmptyQueryResults<T> implements QueryResults<T> {
    @Override
    public Stream<T> getHits() {
        return Stream.empty();
    }

    @Override
    public long getTotalHits() {
        return 0;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return null;
    }

    @Override
    public Double getScore(Object id) {
        return null;
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return 0;
    }
}
