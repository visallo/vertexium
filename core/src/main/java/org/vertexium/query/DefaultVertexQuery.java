package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.JoinIterable;

import java.util.EnumSet;
import java.util.List;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        Iterable<Vertex> vertices = allVertices(fetchHints);
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), vertices, true, true, true, getAggregations());
    }

    private Iterable<Vertex> allVertices(EnumSet<FetchHint> fetchHints) {
        List<String> edgeLabels = getParameters().getEdgeLabels();
        String[] edgeLabelsArray = edgeLabels == null || edgeLabels.size() == 0
                ? null
                : edgeLabels.toArray(new String[edgeLabels.size()]);
        Iterable<Vertex> results = getSourceVertex().getVertices(
                getDirection(),
                edgeLabelsArray,
                fetchHints,
                getParameters().getAuthorizations()
        );
        if (getOtherVertexId() != null) {
            results = new FilterIterable<Vertex>(results) {
                @Override
                protected boolean isIncluded(Vertex otherVertex) {
                    return otherVertex.getId().equals(getOtherVertexId());
                }
            };
        }
        if (getParameters().getIds().size() > 0) {
            results = new FilterIterable<Vertex>(results) {
                @Override
                protected boolean isIncluded(Vertex otherVertex) {
                    return getParameters().getIds().contains(otherVertex.getId());
                }
            };
        }
        return results;
    }

    @Override
    public QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        Iterable<Edge> edges = allEdges(fetchHints);
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), edges, true, true, true, getAggregations());
    }

    private Iterable<Edge> allEdges(EnumSet<FetchHint> fetchHints) {
        Iterable<Edge> results = getSourceVertex().getEdges(getDirection(), fetchHints, getParameters().getAuthorizations());
        if (getOtherVertexId() != null) {
            results = new FilterIterable<Edge>(results) {
                @Override
                protected boolean isIncluded(Edge edge) {
                    return edge.getOtherVertexId(getSourceVertex().getId()).equals(getOtherVertexId());
                }
            };
        }
        return results;
    }

    @Override
    protected QueryResultsIterable<? extends VertexiumObject> extendedData(EnumSet<FetchHint> extendedDataFetchHints) {
        EnumSet<FetchHint> fetchHints = EnumSet.of(FetchHint.EXTENDED_DATA_TABLE_NAMES);
        return extendedData(extendedDataFetchHints, new JoinIterable<>(
                allVertices(fetchHints),
                allEdges(fetchHints)
        ));
    }
}
