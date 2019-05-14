package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.query.Aggregation;
import org.vertexium.search.GraphQueryBase;
import org.vertexium.search.QueryResults;
import org.vertexium.util.StreamUtils;

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
                this.getStreamFromElementType(ElementType.VERTEX, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.<Vertex>getStreamFromElementType(ElementType.VERTEX, idFetchHintsToElementFetchHints(idFetchHints)),
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
                this.getStreamFromElementType(ElementType.EDGE, fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                this.<Edge>getStreamFromElementType(ElementType.EDGE, idFetchHintsToElementFetchHints(idFetchHints)),
                true,
                true,
                true,
                getAggregations(),
                Element::getId
        );
    }

    @Override
    public QueryResults<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                extendedData(fetchHints),
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        return new DefaultGraphQueryResults<>(
                getParameters(),
                extendedData(idFetchHintsToElementFetchHints(idFetchHints)),
                true,
                true,
                true,
                getAggregations(),
                ExtendedDataRow::getId
        );
    }

    @Override
    public QueryResults<Element> elements(FetchHints fetchHints) {
        Stream<Element> elements = Stream.concat(
                getStreamFromElementType(ElementType.VERTEX, fetchHints),
                getStreamFromElementType(ElementType.EDGE, fetchHints)
        );
        return new DefaultGraphQueryResults<>(
                getParameters(),
                elements,
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    @Override
    public QueryResults<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        Stream<Element> elements = Stream.concat(
                getStreamFromElementType(ElementType.VERTEX, fetchHints),
                getStreamFromElementType(ElementType.EDGE, fetchHints)
        );
        return new DefaultGraphQueryResults<>(
                getParameters(),
                elements,
                true,
                true,
                true,
                getAggregations(),
                Element::getId
        );
    }

    @Override
    public QueryResults<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, FetchHints fetchHints) {
        Stream<? extends VertexiumObject> objects = Stream.empty();
        if (objectTypes.contains(VertexiumObjectType.VERTEX)) {
            objects = this.<Vertex>getStreamFromElementType(ElementType.VERTEX, fetchHints);
        }
        if (objectTypes.contains(VertexiumObjectType.EDGE)) {
            objects = Stream.concat(objects, this.<Vertex>getStreamFromElementType(ElementType.EDGE, fetchHints));
        }
        if (objectTypes.contains(VertexiumObjectType.EXTENDED_DATA)) {
            objects = Stream.concat(objects, extendedData(fetchHints));
        }
        return new DefaultGraphQueryResults<>(
                getParameters(),
                objects,
                true,
                true,
                true,
                getAggregations(),
                Function.identity()
        );
    }

    private Stream<ExtendedDataRow> extendedData(FetchHints fetchHints) {
        FetchHints extendedDataTableNamesFetchHints = FetchHints.builder()
                .setIncludeExtendedDataTableNames(true)
                .setIgnoreAdditionalVisibilities(fetchHints.isIgnoreAdditionalVisibilities())
                .build();
        return Stream.concat(
                this.<Vertex>getStreamFromElementType(ElementType.VERTEX, extendedDataTableNamesFetchHints),
                this.<Edge>getStreamFromElementType(ElementType.EDGE, extendedDataTableNamesFetchHints)
        ).flatMap(element ->
                element.getExtendedDataTableNames().stream()
                .flatMap(tableName -> StreamUtils.stream(element.getExtendedData(tableName, fetchHints)))
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends Element> Stream<T> getStreamFromElementType(ElementType elementType, FetchHints fetchHints) throws VertexiumException {
        switch (elementType) {
            case VERTEX:
                return (Stream<T>) getGraph().getVertices(fetchHints, getParameters().getUser());
            case EDGE:
                return (Stream<T>) getGraph().getEdges(fetchHints, getParameters().getUser());
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryResults.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }
}
