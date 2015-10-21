package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.util.SelectManyIterable;

import java.io.IOException;

public abstract class QueryResultsSelectManyIterable<TDest extends Element> extends SelectManyIterable<Query, TDest> implements QueryResultsIterable<TDest> {
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
