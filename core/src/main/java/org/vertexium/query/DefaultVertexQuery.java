package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.*;

import java.util.EnumSet;
import java.util.Map;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, propertyDefinitions, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        Iterable<Vertex> vertices = getSourceVertex().getVertices(Direction.BOTH, fetchHints, getParameters().getAuthorizations());
        return new DefaultGraphQueryIterable<>(getParameters(), vertices, true, true);
    }

    @Override
    public Iterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        Iterable<Edge> edges = getSourceVertex().getEdges(Direction.BOTH, fetchHints, getParameters().getAuthorizations());
        return new DefaultGraphQueryIterable<>(getParameters(), edges, true, true);
    }
}
