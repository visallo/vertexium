package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.scoring.ScoringStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

public class EmptyResultsGraphQuery implements Query {
    private List<Aggregation> aggregations = new ArrayList<>();

    @Override
    public QueryResultsIterable<Vertex> vertices() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Edge> edges() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Edge> edges(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label, final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Element> elements() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Element> elements(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        return this;
    }

    @Override
    public <T> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        return this;
    }

    @Override
    public Query hasId(String... ids) {
        return this;
    }

    @Override
    public Query hasId(Iterable<String> ids) {
        return this;
    }

    @Override
    public Query hasEdgeLabel(String... edgeLabels) {
        return this;
    }

    @Override
    public Query hasEdgeLabel(Collection<String> edgeLabels) {
        return this;
    }

    @Override
    public Query hasAuthorization(String... authorizations) {
        return this;
    }

    @Override
    public Query hasAuthorization(Iterable<String> authorizations) {
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId) {
        return this;
    }

    @Override
    public Query hasExtendedData(String tableName) {
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId, String tableName) {
        return this;
    }

    @Override
    public Query hasExtendedData(Iterable<HasExtendedDataFilter> filters) {
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        return this;
    }

    @Override
    public <T> Query hasNot(String propertyName, T value) {
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        return this;
    }

    @Override
    public <T> Query has(Class dataType, Predicate predicate, T value) {
        return this;
    }

    @Override
    public Query has(Class dataType) {
        return this;
    }

    @Override
    public Query hasNot(Class dataType) {
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames) {
        return this;
    }

    @Override
    public <T> Query hasNot(Iterable<String> propertyNames) {
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames, Predicate predicate, T value) {
        return this;
    }

    @Override
    public Query has(String propertyName) {
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        return this;
    }

    @Override
    public Query skip(int count) {
        return this;
    }

    @Override
    public Query limit(Integer count) {
        return this;
    }

    @Override
    public Query limit(Long count) {
        return this;
    }

    @Override
    public Query minScore(double score) {
        return this;
    }

    @Override
    public Query scoringStrategy(ScoringStrategy scoringStrategy) {
        return this;
    }

    @Override
    public Query sort(String propertyName, SortDirection direction) {
        return this;
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        return false;
    }

    @Override
    public Query addAggregation(Aggregation aggregation) {
        aggregations.add(aggregation);
        return this;
    }

    @Override
    public Iterable<Aggregation> getAggregations() {
        return aggregations;
    }
}
