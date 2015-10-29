package org.vertexium.query;

import org.vertexium.*;

import java.util.EnumSet;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public abstract QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    @Override
    public abstract QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    public Vertex getSourceVertex() {
        return sourceVertex;
    }
}
