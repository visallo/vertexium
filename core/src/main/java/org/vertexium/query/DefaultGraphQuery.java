package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.EnumSet;

public class DefaultGraphQuery extends GraphQueryBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DefaultGraphQuery.class);

    public DefaultGraphQuery(Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        LOGGER.warn("scanning all vertices! create your own GraphQuery.");
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
    public QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        LOGGER.warn("scanning all edges! create your own GraphQuery.");
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
    private <T extends Element> Iterable<T> getIterableFromElementType(ElementType elementType, EnumSet<FetchHint> fetchHints) throws VertexiumException {
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
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryIterableWithAggregations.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }
}
