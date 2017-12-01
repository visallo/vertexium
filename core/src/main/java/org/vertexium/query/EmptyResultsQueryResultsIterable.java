package org.vertexium.query;

import org.vertexium.VertexiumException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class EmptyResultsQueryResultsIterable<T> implements QueryResultsIterable<T> {
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

    @Override
    public Iterator<T> iterator() {
        return new ArrayList<T>().iterator();
    }
}
