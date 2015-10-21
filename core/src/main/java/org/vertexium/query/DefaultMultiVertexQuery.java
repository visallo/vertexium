package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VerticesToEdgeIdsIterable;

import java.util.EnumSet;
import java.util.Map;

public class DefaultMultiVertexQuery extends QueryBase implements MultiVertexQuery {
    private final String[] vertexIds;

    public DefaultMultiVertexQuery(Graph graph, String[] vertexIds, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.vertexIds = vertexIds;
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        Iterable<Vertex> vertices = getGraph().getVertices(IterableUtils.toIterable(getVertexIds()), fetchHints, getParameters().getAuthorizations());
        return new DefaultGraphQueryIterable<>(getParameters(), vertices, true, true, true);
    }

    @Override
    public QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        Iterable<Vertex> vertices = getGraph().getVertices(IterableUtils.toIterable(getVertexIds()), fetchHints, getParameters().getAuthorizations());
        Iterable<String> edgeIds = new VerticesToEdgeIdsIterable(vertices, getParameters().getAuthorizations());
        Iterable<Edge> edges = getGraph().getEdges(edgeIds, fetchHints, getParameters().getAuthorizations());
        return new DefaultGraphQueryIterable<>(getParameters(), edges, true, true, true);
    }

    public String[] getVertexIds() {
        return vertexIds;
    }
}
