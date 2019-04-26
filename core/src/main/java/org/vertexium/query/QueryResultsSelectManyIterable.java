package org.vertexium.query;

import org.vertexium.VertexiumException;
import org.vertexium.VertexiumObject;
import org.vertexium.util.SelectManyIterable;

import java.io.IOException;

public abstract class QueryResultsSelectManyIterable<TDest extends VertexiumObject> extends SelectManyIterable<Query, TDest> implements QueryResultsIterable<TDest> {
    public QueryResultsSelectManyIterable(Iterable<Query> source) {
        super(source);
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        throw new VertexiumException("Not implemented");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getTotalHits() {
        return 0;
    }
}
