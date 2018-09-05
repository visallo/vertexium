package org.vertexium.query;

import org.vertexium.*;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private String otherVertexId;
    private Direction direction = Direction.BOTH;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public abstract QueryResultsIterable<Vertex> vertices(FetchHints fetchHints);

    @Override
    public abstract QueryResultsIterable<Edge> edges(FetchHints fetchHints);

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

    @Override
    public String toString() {
        return super.toString() +
                ", sourceVertex=" + sourceVertex +
                ", otherVertexId=" + otherVertexId +
                ", direction=" + direction;
    }
}
