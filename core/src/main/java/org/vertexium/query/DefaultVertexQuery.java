package org.vertexium.query;

import org.vertexium.*;

import java.util.EnumSet;
import java.util.List;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        List<String> edgeLabels = getParameters().getEdgeLabels();
        String[] edgeLabelsArray = edgeLabels == null || edgeLabels.size() == 0
                ? null
                : edgeLabels.toArray(new String[edgeLabels.size()]);
        Iterable<Vertex> vertices = getSourceVertex().getVertices(
                Direction.BOTH,
                edgeLabelsArray,
                fetchHints,
                getParameters().getAuthorizations()
        );
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), vertices, true, true, true, getAggregations());
    }

    @Override
    public QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        Iterable<Edge> edges = getSourceVertex().getEdges(Direction.BOTH, fetchHints, getParameters().getAuthorizations());
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), edges, true, true, true, getAggregations());
    }
}
