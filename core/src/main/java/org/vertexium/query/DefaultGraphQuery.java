package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.JoinIterable;

public class DefaultGraphQuery extends GraphQueryBase {
    public DefaultGraphQuery(Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(FetchHints fetchHints) {
        return new DefaultGraphQueryIterableWithAggregations<>(
            getParameters(),
            this.<Vertex>getIterableFromElementType(ElementType.VERTEX, fetchHints),
            true,
            true,
            true,
            getAggregations()
        );
    }

    @Override
    public QueryResultsIterable<Edge> edges(FetchHints fetchHints) {
        return new DefaultGraphQueryIterableWithAggregations<>(
            getParameters(),
            this.<Edge>getIterableFromElementType(ElementType.EDGE, fetchHints),
            true,
            true,
            true,
            getAggregations()
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends Element> Iterable<T> getIterableFromElementType(ElementType elementType, FetchHints fetchHints) throws VertexiumException {
        switch (elementType) {
            case VERTEX:
                return (Iterable<T>) getGraph().getVertices(fetchHints, getParameters().getAuthorizations());
            case EDGE:
                return (Iterable<T>) getGraph().getEdges(fetchHints, getParameters().getAuthorizations());
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    @Override
    protected QueryResultsIterable<? extends VertexiumObject> extendedData(FetchHints extendedDataFetchHints) {
        FetchHints extendedDataTableNamesFetchHints = FetchHints.builder()
            .setIncludeExtendedDataTableNames(true)
            .setIgnoreAdditionalVisibilities(extendedDataFetchHints.isIgnoreAdditionalVisibilities())
            .build();
        return extendedData(extendedDataFetchHints, new JoinIterable<>(
            getIterableFromElementType(ElementType.VERTEX, extendedDataTableNamesFetchHints),
            getIterableFromElementType(ElementType.EDGE, extendedDataTableNamesFetchHints)
        ));
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryIterableWithAggregations.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }
}
