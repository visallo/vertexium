package org.vertexium.inmemory.search;

import com.google.common.base.Joiner;
import org.vertexium.*;
import org.vertexium.search.MultiVertexQuery;
import org.vertexium.search.QueryBase;
import org.vertexium.search.QueryResults;
import org.vertexium.util.IterableUtils;

import java.util.EnumSet;
import java.util.stream.Stream;

public class DefaultMultiVertexQuery extends QueryBase implements MultiVertexQuery {
    private final String[] vertexIds;

    public DefaultMultiVertexQuery(Graph graph, String[] vertexIds, String queryString, User user) {
        super(graph, queryString, user);
        this.vertexIds = vertexIds;
    }

    @Override
    public QueryResults<Vertex> vertices(FetchHints fetchHints) {
        Stream<Vertex> vertices = getGraph().getVertices(IterableUtils.toIterable(getVertexIds()), fetchHints, getParameters().getUser());
        return new DefaultGraphQueryResultsWithAggregations<>(getParameters(), vertices, true, true, true, getAggregations());
    }

    @Override
    public QueryResults<String> vertexIds(EnumSet<IdFetchHint> fetchHints) {
        return null;
    }

    @Override
    public QueryResults<Edge> edges(FetchHints fetchHints) {
        Stream<Edge> edges = getGraph().getVertices(IterableUtils.toIterable(getVertexIds()), fetchHints, getParameters().getUser())
                .flatMap(v -> v.getEdges(Direction.BOTH, fetchHints, getParameters().getUser()));
        return new DefaultGraphQueryResultsWithAggregations<>(getParameters(), edges, true, true, true, getAggregations());
    }

    @Override
    public QueryResults<String> edgeIds(EnumSet<IdFetchHint> fetchHints) {
        return null;
    }

    @Override
    public QueryResults<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> fetchHints) {
        return null;
    }

    @Override
    public QueryResults<String> elementIds(EnumSet<IdFetchHint> fetchHints) {
        return null;
    }

    @Override
    public QueryResults<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, FetchHints fetchHints) {
        return null;
    }


//    @Override
//    protected QueryResultsIterable<? extends VertexiumObject> extendedData(FetchHints fetchHints) {
//        Iterable<Vertex> vertices = toIterable(getGraph().getVertices(IterableUtils.toIterable(getVertexIds()), fetchHints, getParameters().getUser()));
//        Iterable<String> edgeIds = new VerticesToEdgeIdsIterable(vertices, getParameters().getUser());
//        Iterable<Edge> edges = toIterable(getGraph().getEdges(edgeIds, fetchHints, getParameters().getUser()));
//        return extendedData(fetchHints, new JoinIterable<>(vertices, edges));
//    }

    public String[] getVertexIds() {
        return vertexIds;
    }

    @Override
    public String toString() {
        return super.toString() +
            ", vertexIds=" + Joiner.on(", ").join(vertexIds);
    }
}
