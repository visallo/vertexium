package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.util.CloseableIterable;
import org.vertexium.util.JoinIterable;

import java.io.IOException;

public class QueryResultsJoinIterable<T extends Element> extends JoinIterable<T> implements QueryResultsIterable<T> {
    @SuppressWarnings("unchecked")
    public QueryResultsJoinIterable(Iterable<T>... iterables) {
        super(iterables);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        for (Iterable<? extends T> iterable : getIterables()) {
            if (iterable instanceof QueryResultsIterable) {
                TResult aggResult = ((QueryResultsIterable<T>) iterable).getAggregationResult(name, resultType);
                if (aggResult != null) {
                    return aggResult;
                }
            }
        }
        return AggregationResult.createEmptyResult(resultType);
    }

    @Override
    public void close() throws IOException {
        for (Iterable<? extends T> iterable : getIterables()) {
            if (iterable instanceof CloseableIterable) {
                ((CloseableIterable) iterable).close();
            }
        }
    }

    @Override
    public long getTotalHits() {
        long total = 0;
        for (Iterable<? extends T> iterable : getIterables()) {
            if (iterable instanceof IterableWithTotalHits) {
                total += ((IterableWithTotalHits) iterable).getTotalHits();
            }
        }
        return total;
    }
}
