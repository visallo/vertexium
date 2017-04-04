package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.ExtendedDataRow;
import org.vertexium.VertexiumException;
import org.vertexium.VertexiumObject;
import org.vertexium.util.ConvertingIterable;

import java.io.IOException;

public class DefaultGraphQueryIdIterable<T> extends ConvertingIterable<VertexiumObject, T> implements QueryResultsIterable<T> {

    private final QueryResultsIterable<? extends VertexiumObject> iterable;

    public DefaultGraphQueryIdIterable(QueryResultsIterable<? extends VertexiumObject> iterable) {
        super(iterable);
        this.iterable = iterable;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T convert(VertexiumObject vertexiumObject) {
        if (vertexiumObject instanceof Element) {
            return (T) ((Element) vertexiumObject).getId();
        } else if (vertexiumObject instanceof ExtendedDataRow) {
            return (T) ((ExtendedDataRow) vertexiumObject).getId();
        }
        throw new VertexiumException("Unsupported class: " + vertexiumObject.getClass().getName());
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return iterable.getAggregationResult(name, resultType);
    }

    @Override
    public void close() throws IOException {
        iterable.close();
    }

    @Override
    public long getTotalHits() {
        return iterable.getTotalHits();
    }
}
