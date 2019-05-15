package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.ExistingEdgeMutation;

public class ElasticsearchEdge extends ElasticsearchElement implements Edge {
    private String label;
    private String inVertexId;
    private String outVertexId;

    public ElasticsearchEdge(
        Graph graph,
        String id,
        String label,
        String inVertexId,
        String outVertexId,
        FetchHints fetchHints,
        User user
    ) {
        super(graph, id, fetchHints, user);
        this.label = label;
        this.inVertexId = inVertexId;
        this.outVertexId = outVertexId;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId(Direction direction) {
        if (direction.equals(Direction.IN)) {
            return inVertexId;
        } else if (direction.equals(Direction.OUT)) {
            return outVertexId;
        }
        throw new VertexiumNotSupportedException(direction.name() + " is not supported");
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertex is not supported");
    }

    @Override
    public Vertex getVertex(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertex is not supported");
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (myVertexId.equals(inVertexId)) {
            return outVertexId;
        }
        return inVertexId;
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getOtherVertex is not supported");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getOtherVertex is not supported");
    }

    @Override
    public EdgeVertices getVertices(Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported");
    }

    @Override
    public EdgeVertices getVertices(FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported");
    }

    @Override
    public ExistingEdgeMutation prepareMutation() {
        throw new VertexiumNotSupportedException("prepareMutation is not supported");
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }
}
