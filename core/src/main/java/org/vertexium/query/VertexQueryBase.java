package org.vertexium.query;

import org.vertexium.*;

import java.util.EnumSet;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private String otherVertexId;
    private Direction direction = Direction.BOTH;

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

    @Override
    public VertexQuery hasDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public VertexQuery hasOtherVertexId(String otherVertexId) {
        this.otherVertexId = otherVertexId;
        return this;
    }

    public String getOtherVertexId() {
        return otherVertexId;
    }

    public Direction getDirection() {
        return direction;
    }
}
