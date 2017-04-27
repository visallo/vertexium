package org.vertexium.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.util.SelectManyIterable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

public abstract class QueryBase implements Query, SimilarToGraphQuery {
    private final Graph graph;
    private final QueryParameters parameters;
    private List<Aggregation> aggregations = new ArrayList<>();

    protected QueryBase(Graph graph, String queryString, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = new QueryStringQueryParameters(queryString, authorizations);
    }

    protected QueryBase(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = new SimilarToTextQueryParameters(similarToFields, similarToText, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices() {
        return vertices(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(final EnumSet<FetchHint> fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Vertex>) search(EnumSet.of(VertexiumObjectType.VERTEX), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return new DefaultGraphQueryIdIterable<>(vertices(FetchHint.NONE));
    }

    @Override
    public QueryResultsIterable<Edge> edges() {
        return edges(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Edge> edges(final EnumSet<FetchHint> fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Edge>) search(EnumSet.of(VertexiumObjectType.EDGE), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return new DefaultGraphQueryIdIterable<>(edges(FetchHint.NONE));
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows() {
        return extendedDataRows(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows(EnumSet<FetchHint> fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<ExtendedDataRow>) search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA), fetchHints);
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search() {
        return search(VertexiumObjectType.ALL, getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, EnumSet<FetchHint> fetchHints) {
        List<QueryResultsIterable<? extends VertexiumObject>> items = new ArrayList<>();
        if (objectTypes.contains(VertexiumObjectType.VERTEX)) {
            items.add(vertices(fetchHints));
        }
        if (objectTypes.contains(VertexiumObjectType.EDGE)) {
            items.add(edges(fetchHints));
        }
        if (objectTypes.contains(VertexiumObjectType.EXTENDED_DATA)) {
            items.add(extendedData(fetchHints));
        }

        return new SelectManySearch(items);
    }

    private static class SelectManySearch
            extends SelectManyIterable<QueryResultsIterable<? extends VertexiumObject>, VertexiumObject>
            implements QueryResultsIterable<VertexiumObject> {
        public SelectManySearch(Iterable<? extends QueryResultsIterable<? extends VertexiumObject>> source) {
            super(source);
        }

        @Override
        public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
            throw new VertexiumException("Not implemented");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long getTotalHits() {
            long totalHits = 0;
            for (QueryResultsIterable queryResultsIterable : getSource()) {
                totalHits += queryResultsIterable.getTotalHits();
            }
            return totalHits;
        }

        @Override
        protected Iterable<? extends VertexiumObject> getIterable(QueryResultsIterable<? extends VertexiumObject> source) {
            return source;
        }
    }

    /**
     * This method should be overridden if {@link #search(EnumSet, EnumSet)} is not overridden.
     */
    protected QueryResultsIterable<? extends VertexiumObject> extendedData(EnumSet<FetchHint> fetchHints) {
        throw new VertexiumException("not implemented");
    }

    protected QueryResultsIterable<? extends VertexiumObject> extendedData(EnumSet<FetchHint> fetchHints, Iterable<? extends Element> elements) {
        Iterable<ExtendedDataRow> allExtendedData = new SelectManyIterable<Element, ExtendedDataRow>(elements) {
            @Override
            protected Iterable<? extends ExtendedDataRow> getIterable(Element element) {
                return new SelectManyIterable<String, ExtendedDataRow>(element.getExtendedDataTableNames()) {
                    @Override
                    protected Iterable<? extends ExtendedDataRow> getIterable(String tableName) {
                        return element.getExtendedData(tableName);
                    }
                };
            }
        };
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), allExtendedData, true, true, true, getAggregations());
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds() {
        QueryResultsIterable<? extends VertexiumObject> vertexiumObjects = search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA), FetchHint.NONE);
        return new DefaultGraphQueryIdIterable<>(vertexiumObjects);
    }

    @Override
    public Query hasEdgeLabel(String... edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    @Override
    public Query hasEdgeLabel(Collection<String> edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId) {
        return hasExtendedData(elementType, elementId, null);
    }

    @Override
    public Query hasExtendedData(String tableName) {
        return hasExtendedData(null, null, tableName);
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId, String tableName) {
        hasExtendedData(Lists.newArrayList(new HasExtendedDataFilter(elementType, elementId, tableName)));
        return this;
    }

    @Override
    public Query hasExtendedData(Iterable<HasExtendedDataFilter> filters) {
        getParameters().addHasContainer(new HasExtendedData(ImmutableList.copyOf(filters)));
        return this;
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label, EnumSet<FetchHint> fetchHints) {
        hasEdgeLabel(label);
        return edges(fetchHints);
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label) {
        hasEdgeLabel(label);
        return edges();
    }

    @Override
    public QueryResultsIterable<Element> elements() {
        return elements(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Element> elements(EnumSet<FetchHint> fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Element>) search(VertexiumObjectType.ELEMENTS, fetchHints);
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return new DefaultGraphQueryIdIterable<>(elements(FetchHint.NONE));
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        return range(propertyName, startValue, true, endValue, true);
    }

    @Override
    public <T> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        if (startValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveStartValue ? Compare.GREATER_THAN_EQUAL : Compare.GREATER_THAN, startValue, getGraph().getPropertyDefinitions()));
        }
        if (endValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(propertyName, inclusiveEndValue ? Compare.LESS_THAN_EQUAL : Compare.LESS_THAN, endValue, getGraph().getPropertyDefinitions()));
        }
        return this;
    }

    @Override
    public Query sort(String propertyName, SortDirection direction) {
        this.parameters.addSortContainer(new SortContainer(propertyName, direction));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Compare.EQUAL, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> Query hasNot(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, Contains.NOT_IN, new Object[]{value}, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyName, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public Query has(String propertyName) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyName));
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyName));
        return this;
    }

    @Override
    public Query skip(int count) {
        this.parameters.setSkip(count);
        return this;
    }

    @Override
    public Query limit(Integer count) {
        this.parameters.setLimit(count);
        return this;
    }

    @Override
    public Query limit(Long count) {
        this.parameters.setLimit(count);
        return this;
    }

    public Graph getGraph() {
        return graph;
    }

    public QueryParameters getParameters() {
        return parameters;
    }

    public static abstract class HasContainer {
        public abstract boolean isMatch(VertexiumObject elem);

        @Override
        public String toString() {
            return this.getClass().getName() + "{}";
        }
    }

    private static abstract class HasContainerSplitElementExtendedDataRows extends HasContainer {
        @Override
        public boolean isMatch(VertexiumObject vertexiumObject) {
            if (vertexiumObject instanceof Element) {
                return isMatch((Element) vertexiumObject);
            } else if (vertexiumObject instanceof ExtendedDataRow) {
                return isMatch((ExtendedDataRow) vertexiumObject);
            } else {
                throw new VertexiumException("Unhandled VertexiumObject type: " + vertexiumObject.getClass().getName());
            }
        }

        protected abstract boolean isMatch(Element element);

        protected abstract boolean isMatch(ExtendedDataRow row);
    }

    public static class SortContainer {
        public final String propertyName;
        public final SortDirection direction;

        public SortContainer(String propertyName, SortDirection direction) {
            this.propertyName = propertyName;
            this.direction = direction;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "propertyName='" + propertyName + '\'' +
                    ", direction=" + direction +
                    '}';
        }
    }

    public static class HasValueContainer extends HasContainerSplitElementExtendedDataRows {
        public String key;
        public Object value;
        public Predicate predicate;
        private final Collection<PropertyDefinition> propertyDefinitions;

        public HasValueContainer(final String key, final Predicate predicate, final Object value, Collection<PropertyDefinition> propertyDefinitions) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
            this.propertyDefinitions = propertyDefinitions;
        }

        @Override
        protected boolean isMatch(ExtendedDataRow extendedDataRow) {
            Iterable<String> propertyNames = extendedDataRow.getPropertyNames();
            for (String propertyName : propertyNames) {
                if (propertyName.equals(this.key)) {
                    PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(this.propertyDefinitions, propertyName);
                    Object columnValue = extendedDataRow.getPropertyValue(propertyName);
                    if (this.predicate.evaluate(columnValue, this.value, propertyDefinition)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        protected boolean isMatch(Element element) {
            return this.predicate.evaluate(element.getProperties(this.key), this.value, this.propertyDefinitions);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "predicate=" + predicate +
                    ", value=" + value +
                    ", key='" + key + '\'' +
                    '}';
        }
    }

    public static class HasExtendedData extends HasContainer {
        private final ImmutableList<HasExtendedDataFilter> filters;

        public HasExtendedData(ImmutableList<HasExtendedDataFilter> filters) {
            this.filters = filters;
        }

        public ImmutableList<HasExtendedDataFilter> getFilters() {
            return filters;
        }

        @Override
        public boolean isMatch(VertexiumObject elem) {
            if (!(elem instanceof ExtendedDataRow)) {
                return false;
            }

            ExtendedDataRow row = (ExtendedDataRow) elem;
            ExtendedDataRowId rowId = row.getId();
            for (HasExtendedDataFilter filter : filters) {
                if (filter.getElementType() == null || rowId.getElementType().equals(filter.getElementType())
                        && (filter.getElementId() == null || rowId.getElementId().equals(filter.getElementId()))
                        && (filter.getTableName() == null || rowId.getTableName().equals(filter.getTableName()))) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class HasPropertyContainer extends HasContainerSplitElementExtendedDataRows {
        private final String key;

        public HasPropertyContainer(String key) {
            this.key = key;
        }

        @Override
        protected boolean isMatch(ExtendedDataRow row) {
            for (String propertyName : row.getPropertyNames()) {
                if (propertyName.equals(this.key)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean isMatch(Element element) {
            for (Property prop : element.getProperties()) {
                if (prop.getName().equals(this.key)) {
                    return true;
                }
            }
            return false;
        }

        public String getKey() {
            return key;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "key='" + key + '\'' +
                    '}';
        }
    }

    public static class HasNotPropertyContainer extends HasContainerSplitElementExtendedDataRows {
        private final String key;

        public HasNotPropertyContainer(String key) {
            this.key = key;
        }

        @Override
        protected boolean isMatch(ExtendedDataRow row) {
            for (String propertyName : row.getPropertyNames()) {
                if (propertyName.equals(this.key)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected boolean isMatch(Element element) {
            for (Property prop : element.getProperties()) {
                if (prop.getName().equals(this.key)) {
                    return false;
                }
            }
            return true;
        }

        public String getKey() {
            return key;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "key='" + key + '\'' +
                    '}';
        }
    }

    @Override
    public SimilarToGraphQuery minTermFrequency(int minTermFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinTermFrequency(minTermFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxQueryTerms(int maxQueryTerms) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxQueryTerms(maxQueryTerms);
        return this;
    }

    @Override
    public SimilarToGraphQuery minDocFrequency(int minDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinDocFrequency(minDocFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxDocFrequency(int maxDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxDocFrequency(maxDocFrequency);
        return this;
    }

    /**
     * @deprecated As of 2.6.0 this call has no effect in Elasticsearch and will be remove
     */
    @Override
    @Deprecated
    public SimilarToGraphQuery percentTermsToMatch(float percentTermsToMatch) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new VertexiumException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setPercentTermsToMatch(percentTermsToMatch);
        return this;
    }

    @Override
    public SimilarToGraphQuery boost(float boost) {
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
}
