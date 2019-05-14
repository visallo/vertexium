package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.query.Aggregation;
import org.vertexium.search.GraphQueryBase;
import org.vertexium.search.QueryResults;

import java.util.EnumSet;
import java.util.function.Function;
import java.util.stream.Stream;

public class DefaultGraphQuery extends GraphQueryBase {
    public DefaultGraphQuery(Graph graph, String queryString, User user) {
        super(graph, queryString, user);
    }

    @Override
    public QueryResults<Vertex> vertices(FetchHints fetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.getIterableFromElementType(ElementType.VERTEX, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHints == IdFetchHint.ALL_INCLUDING_HIDDEN ? FetchHints.ALL : FetchHints.NONE;
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.<Vertex>getIterableFromElementType(ElementType.VERTEX, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Element::getId
        );
    }

    @Override
    public QueryResults<Edge> edges(FetchHints fetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.<Edge>getIterableFromElementType(ElementType.EDGE, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHints == IdFetchHint.ALL_INCLUDING_HIDDEN ? FetchHints.ALL : FetchHints.NONE;
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.<Edge>getIterableFromElementType(ElementType.EDGE, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Element::getId
        );
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

    @SuppressWarnings("unchecked")
    private <T extends Element> Stream<T> getIterableFromElementType(ElementType elementType, FetchHints fetchHints) throws VertexiumException {
        switch (elementType) {
            case VERTEX:
                return (Stream<T>) getGraph().getVertices(fetchHints, getParameters().getUser());
            case EDGE:
                return (Stream<T>) getGraph().getEdges(fetchHints, getParameters().getUser());
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

//    @Override
//    protected QueryResults<? extends VertexiumObject> extendedData(FetchHints extendedDataFetchHints) {
//        FetchHints extendedDataTableNamesFetchHints = FetchHints.builder()
//                .setIncludeExtendedDataTableNames(true)
//                .setIgnoreAdditionalVisibilities(extendedDataFetchHints.isIgnoreAdditionalVisibilities())
//                .build();
//        return extendedData(extendedDataFetchHints, new JoinIterable<>(
//                getIterableFromElementType(ElementType.VERTEX, extendedDataTableNamesFetchHints),
//                getIterableFromElementType(ElementType.EDGE, extendedDataTableNamesFetchHints)
//        ));
//    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryResults.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }
}
