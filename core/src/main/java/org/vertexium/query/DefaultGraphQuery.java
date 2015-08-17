package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.EnumSet;
import java.util.Map;

public class DefaultGraphQuery extends GraphQueryBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DefaultGraphQuery.class);

    public DefaultGraphQuery(Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        LOGGER.warn("scanning all vertices! create your own GraphQuery.");
        return new DefaultGraphQueryIterable<>(getParameters(), this.<Vertex>getIterableFromElementType(ElementType.VERTEX, fetchHints), true, true, true);
    }

    @Override
    public Iterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        LOGGER.warn("scanning all edges! create your own GraphQuery.");
        return new DefaultGraphQueryIterable<>(getParameters(), this.<Edge>getIterableFromElementType(ElementType.EDGE, fetchHints), true, true, true);
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
}
