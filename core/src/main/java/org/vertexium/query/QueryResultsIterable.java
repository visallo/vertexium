package org.vertexium.query;

import org.vertexium.VertexiumObject;
import org.vertexium.util.CloseableIterable;

public interface QueryResultsIterable<T extends VertexiumObject> extends
        IterableWithTotalHits<T>,
        CloseableIterable<T>,
        Iterable<T> {
    <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType);
}
