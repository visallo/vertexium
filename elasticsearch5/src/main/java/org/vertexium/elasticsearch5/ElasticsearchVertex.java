package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;

import java.util.stream.Stream;

public class ElasticsearchVertex extends ElasticsearchElement implements Vertex {
    public ElasticsearchVertex(
        Graph graph,
        String id,
        FetchHints fetchHints,
        User user
    ) {
        super(graph, id, fetchHints, user);
    }

    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        return super.prepareMutation();
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    public Stream<Edge> getEdges(
        Vertex otherVertex,
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Long endTime, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }
}
