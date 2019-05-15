package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.VertexQuery;

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
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        return super.prepareMutation();
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, User user) {
        throw new VertexiumNotSupportedException("not supported on " + getClass().getSimpleName());
    }
}
