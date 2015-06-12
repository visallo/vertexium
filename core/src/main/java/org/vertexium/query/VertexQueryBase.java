package org.vertexium.query;

import org.vertexium.*;

import java.util.EnumSet;
import java.util.Map;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public abstract Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    @Override
    public abstract Iterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    public Vertex getSourceVertex() {
        return sourceVertex;
    }
}
