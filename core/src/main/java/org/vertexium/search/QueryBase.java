package org.vertexium.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.query.*;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.util.IterableUtils;

import java.util.*;

import static org.vertexium.query.QueryBase.*;

public abstract class QueryBase implements org.vertexium.search.Query, org.vertexium.search.SimilarToGraphQuery {
    private final Graph graph;
    private final QueryParameters parameters;
    private List<Aggregation> aggregations = new ArrayList<>();

    protected QueryBase(Graph graph, String queryString, User user) {
        this.graph = graph;
        this.parameters = new QueryStringQueryParameters(queryString, user);
    }

    protected QueryBase(Graph graph, String[] similarToFields, String similarToText, User user) {
        this.graph = graph;
        this.parameters = new SimilarToTextQueryParameters(similarToFields, similarToText, user);
    }

    @Override
    public QueryResults<Vertex> vertices() {
        return vertices(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResults<Vertex> vertices(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResults<Vertex>) search(EnumSet.of(VertexiumObjectType.VERTEX), fetchHints);
    }

    @Override
    public QueryResults<String> vertexIds() {
        return vertexIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResults<Edge> edges() {
        return edges(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResults<Edge> edges(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResults<Edge>) search(EnumSet.of(VertexiumObjectType.EDGE), fetchHints);
    }

    @Override
    public QueryResults<String> edgeIds() {
        return edgeIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResults<ExtendedDataRow> extendedDataRows() {
        return extendedDataRows(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResults<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResults<ExtendedDataRow>) search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA), fetchHints);
    }

    @Override
    public QueryResults<? extends VertexiumObject> search() {
        return search(VertexiumObjectType.ALL, getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResults<ExtendedDataRowId> extendedDataRowIds() {
        return extendedDataRowIds(IdFetchHint.NONE);
    }

    @Override
    public org.vertexium.search.Query hasEdgeLabel(String... edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    @Override
    public org.vertexium.search.Query hasEdgeLabel(Collection<String> edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    @Override
    public org.vertexium.search.Query hasId(String... ids) {
        getParameters().addIds(Arrays.asList(ids));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasId(Iterable<String> ids) {
        getParameters().addIds(IterableUtils.toList(ids));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasAuthorization(String... authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(Arrays.asList(authorizations)));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasAuthorization(Iterable<String> authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(authorizations));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasExtendedData(ElementType elementType, String elementId) {
        return hasExtendedData(elementType, elementId, null);
    }

    @Override
    public org.vertexium.search.Query hasExtendedData(String tableName) {
        return hasExtendedData(null, null, tableName);
    }

    @Override
    public org.vertexium.search.Query hasExtendedData(ElementType elementType, String elementId, String tableName) {
        hasExtendedData(Lists.newArrayList(new HasExtendedDataFilter(elementType, elementId, tableName)));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasExtendedData(Iterable<HasExtendedDataFilter> filters) {
        getParameters().addHasContainer(new org.vertexium.query.QueryBase.HasExtendedData(ImmutableList.copyOf(filters)));
        return this;
    }

    @Override
    public QueryResults<Element> elements() {
        return elements(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResults<Element> elements(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResults<Element>) search(VertexiumObjectType.ELEMENTS, fetchHints);
    }

    @Override
    public QueryResults<String> elementIds() {
        return elementIds(IdFetchHint.NONE);
    }

    @Override
    public <T> org.vertexium.search.Query range(String propertyName, T startValue, T endValue) {
        return range(propertyName, startValue, true, endValue, true);
    }

    @Override
    public <T> org.vertexium.search.Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        if (startValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveStartValue ? Compare.GREATER_THAN_EQUAL : Compare.GREATER_THAN, startValue, getGraph().getPropertyDefinitions()));
        }
        if (endValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveEndValue ? Compare.LESS_THAN_EQUAL : Compare.LESS_THAN, endValue, getGraph().getPropertyDefinitions()));
        }
        return this;
    }

    @Override
    public org.vertexium.search.Query sort(String propertyName, SortDirection direction) {
        this.parameters.addSortContainer(new SortContainer(propertyName, direction));
        return this;
    }

    @Override
    public <T> org.vertexium.search.Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Compare.EQUAL, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> org.vertexium.search.Query hasNot(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Contains.NOT_IN, new Object[]{value}, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> org.vertexium.search.Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> org.vertexium.search.Query has(Class dataType, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(dataType, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public org.vertexium.search.Query has(Class dataType) {
        this.parameters.addHasContainer(new HasPropertyContainer(dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasNot(Class dataType) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> org.vertexium.search.Query has(Iterable<String> propertyNames, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyNames, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public org.vertexium.search.Query has(String propertyName) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyName));
        return this;
    }

    @Override
    public org.vertexium.search.Query has(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyNames));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasNot(String propertyName) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyName));
        return this;
    }

    @Override
    public org.vertexium.search.Query hasNot(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyNames));
        return this;
    }

    @Override
    public org.vertexium.search.Query skip(int count) {
        this.parameters.setSkip(count);
        return this;
    }

    @Override
    public org.vertexium.search.Query limit(Integer count) {
        this.parameters.setLimit(count);
        return this;
    }

    @Override
    public org.vertexium.search.Query limit(Long count) {
        this.parameters.setLimit(count);
        return this;
    }

    @Override
    public org.vertexium.search.Query minScore(double score) {
        this.parameters.setMinScore(score);
        return this;
    }

    @Override
    public org.vertexium.search.Query scoringStrategy(ScoringStrategy scoringStrategy) {
        this.parameters.setScoringStrategy(scoringStrategy);
        return this;
    }

    public Graph getGraph() {
        return graph;
    }

    public QueryParameters getParameters() {
        return parameters;
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery minTermFrequency(int minTermFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinTermFrequency(minTermFrequency);
        return this;
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery maxQueryTerms(int maxQueryTerms) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxQueryTerms(maxQueryTerms);
        return this;
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery minDocFrequency(int minDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinDocFrequency(minDocFrequency);
        return this;
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery maxDocFrequency(int maxDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxDocFrequency(maxDocFrequency);
        return this;
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery boost(float boost) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setBoost(boost);
        return this;
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        return false;
    }

    @Override
    public Query addAggregation(Aggregation aggregation) {
        if (!isAggregationSupported(aggregation)) {
            throw new VertexiumException("Aggregation " + aggregation.getClass().getName() + " is not supported");
        }
        this.aggregations.add(aggregation);
        return this;
    }

    public Collection<Aggregation> getAggregations() {
        return aggregations;
    }

    public Aggregation getAggregationByName(String aggregationName) {
        for (Aggregation agg : aggregations) {
            if (agg.getAggregationName().equals(aggregationName)) {
                return agg;
            }
        }
        return null;
    }

    protected FetchHints idFetchHintsToElementFetchHints(EnumSet<IdFetchHint> idFetchHints) {
        return idFetchHints.contains(IdFetchHint.INCLUDE_HIDDEN) ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "parameters=" + getParameters() +
                '}';
    }
}
