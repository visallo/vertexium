package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.VertexQuery;

public class ElasticsearchVertex extends ElasticsearchElement implements Vertex {
    private String className = ElasticsearchElement.class.getSimpleName();

    public ElasticsearchVertex(
        Graph graph,
        String id,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        super(graph, id, fetchHints, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgesSummary is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getProperties is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertexIds is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertexIds is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getVertexIds is not supported on " + className);
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
        throw new VertexiumNotSupportedException("prepareMutation is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }
}
