package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.search.VertexQuery;

import java.util.List;
import java.util.stream.Stream;

public class DefaultVertexQuery extends DefaultGraphQuery implements VertexQuery {
    private final Vertex sourceVertex;
    private String otherVertexId;
    private Direction direction = Direction.BOTH;

    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, User user) {
        super(graph, queryString, user);
        this.sourceVertex = sourceVertex;
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

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends Element> Stream<T> getStreamFromElementType(ElementType elementType, FetchHints fetchHints) throws VertexiumException {
        switch (elementType) {
            case VERTEX:
                List<String> edgeLabels = getParameters().getEdgeLabels();
                String[] edgeLabelsArray = null;
                if (edgeLabels != null && !edgeLabels.isEmpty()) {
                    edgeLabelsArray = edgeLabels.toArray(new String[0]);
                }
                Stream<Vertex> vertices = sourceVertex.getVertices(direction, edgeLabelsArray, fetchHints, getParameters().getUser());
                if (otherVertexId != null) {
                    vertices = vertices.filter(v -> v.getId().equals(otherVertexId));
                }
                if (getParameters().getIds() != null) {
                    vertices = vertices.filter(v -> getParameters().getIds().contains(v.getId()));
                }
                return (Stream<T>) vertices;
            case EDGE:
                Stream<Edge> edges = sourceVertex.getEdges(direction, fetchHints, getParameters().getUser());
                if (otherVertexId != null) {
                    edges = edges.filter(e -> e.getOtherVertexId(sourceVertex.getId()).equals(otherVertexId));
                }
                return (Stream<T>) edges;
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    @Override
    public String toString() {
        return super.toString() +
                ", sourceVertex=" + sourceVertex +
                ", otherVertexId=" + otherVertexId +
                ", direction=" + direction;
    }
}
