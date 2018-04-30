package org.vertexium.query;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.type.GeoShape;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.SelectManyIterable;
import org.vertexium.util.StreamUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    public QueryResultsIterable<Vertex> vertices(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Vertex>) search(EnumSet.of(VertexiumObjectType.VERTEX), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return vertexIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(vertices(fetchHints));
    }

    @Override
    public QueryResultsIterable<Edge> edges() {
        return edges(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Edge> edges(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Edge>) search(EnumSet.of(VertexiumObjectType.EDGE), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return edgeIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(edges(fetchHints));
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows() {
        return extendedDataRows(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<ExtendedDataRow>) search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA), fetchHints);
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search() {
        return search(VertexiumObjectType.ALL, getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, FetchHints fetchHints) {
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

        if (items.size() == 1) {
            return items.get(0);
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
    protected QueryResultsIterable<? extends VertexiumObject> extendedData(FetchHints fetchHints) {
        throw new VertexiumException("not implemented");
    }

    protected QueryResultsIterable<? extends VertexiumObject> extendedData(FetchHints fetchHints, Iterable<? extends Element> elements) {
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
        return extendedDataRowIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        QueryResultsIterable<? extends VertexiumObject> vertexiumObjects = search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA), fetchHints);
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
    public Query hasId(String... ids) {
        getParameters().addIds(Arrays.asList(ids));
        return this;
    }

    @Override
    public Query hasId(Iterable<String> ids) {
        getParameters().addIds(IterableUtils.toList(ids));
        return this;
    }

    @Override
    public Query hasAuthorization(String... authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(Arrays.asList(authorizations)));
        return this;
    }

    @Override
    public Query hasAuthorization(Iterable<String> authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(authorizations));
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
    public QueryResultsIterable<Edge> edges(final String label, FetchHints fetchHints) {
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
    public QueryResultsIterable<Element> elements(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Element>) search(VertexiumObjectType.ELEMENTS, fetchHints);
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return elementIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(elements(fetchHints));
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
    public <T> Query has(Class dataType, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(dataType, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> Query has(Class dataType) {
        this.parameters.addHasContainer(new HasPropertyContainer(dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> Query hasNot(Class dataType) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(propertyNames, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public Query has(String propertyName) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyName));
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasPropertyContainer(propertyNames));
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyName));
        return this;
    }

    @Override
    public <T> Query hasNot(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(propertyNames));
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

        protected boolean isPropertyOfType(PropertyDefinition propertyDefinition, Class dataType) {
            boolean propertyIsDate = DateOnly.class.isAssignableFrom(propertyDefinition.getDataType()) || Date.class.isAssignableFrom(propertyDefinition.getDataType());
            boolean dataTypeIsDate = DateOnly.class.isAssignableFrom(dataType) || Date.class.isAssignableFrom(dataType);

            return dataType.isAssignableFrom(propertyDefinition.getDataType()) || (propertyIsDate && dataTypeIsDate);
        }
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

    public static class HasAuthorizationContainer extends HasContainerSplitElementExtendedDataRows {
        public final Set<String> authorizations;

        public HasAuthorizationContainer(Iterable<String> authorizations) {
            this.authorizations = IterableUtils.toSet(authorizations);
        }

        @Override
        protected boolean isMatch(Element element) {
            for (String authorization : authorizations) {
                if (element.getVisibility().hasAuthorization(authorization)) {
                    return true;
                }

                boolean hiddenVisibilityMatches = StreamUtils.stream(element.getHiddenVisibilities())
                        .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                if (hiddenVisibilityMatches) {
                    return true;
                }

                boolean propertyMatches = StreamUtils.stream(element.getProperties())
                        .anyMatch(property -> {
                            if (property.getVisibility().hasAuthorization(authorization)) {
                                return true;
                            }
                            return StreamUtils.stream(property.getHiddenVisibilities())
                                    .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                        });
                if (propertyMatches) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean isMatch(ExtendedDataRow row) {
            for (String authorization : authorizations) {
                boolean propertyMatches = StreamSupport.stream(row.getProperties().spliterator(), false)
                        .anyMatch(property -> property.getVisibility().hasAuthorization(authorization));
                if (propertyMatches) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getAuthorizations() {
            return authorizations;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", authorizations='" + Joiner.on(", ").join(authorizations) + '\'' +
                    '}';
        }
    }

    public static class HasValueContainer extends HasContainerSplitElementExtendedDataRows {
        public final Set<String> keys;
        public final Object value;
        public final Predicate predicate;
        private final Collection<PropertyDefinition> propertyDefinitions;

        public HasValueContainer(String key, Predicate predicate, Object value, Collection<PropertyDefinition> propertyDefinitions) {
            this(Collections.singleton(key), predicate, value, propertyDefinitions);
        }

        public HasValueContainer(Iterable<String> keys, Predicate predicate, Object value, Collection<PropertyDefinition> propertyDefinitions) {
            this.keys = IterableUtils.toSet(keys);
            this.value = value;
            this.predicate = predicate;
            this.propertyDefinitions = propertyDefinitions;

            if (this.keys.isEmpty()) {
                throw new VertexiumException("Invalid query parameters, no property names specified");
            }
            validateParameters();
        }

        public HasValueContainer(Class dataType, Predicate predicate, Object value, Collection<PropertyDefinition> propertyDefinitions) {
            this.value = value;
            this.predicate = predicate;
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());
            this.propertyDefinitions = propertyDefinitions;

            if (this.keys.isEmpty()) {
                throw new VertexiumException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
            validateParameters();
        }

        private void validateParameters() {
            this.keys.forEach(key -> {
                PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(propertyDefinitions, key);
                if (predicate instanceof TextPredicate && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    throw new VertexiumException("Check your TextIndexHint settings. Property " + propertyDefinition.getPropertyName() + " is not full text indexed.");
                } else if (predicate instanceof GeoCompare && !isPropertyOfType(propertyDefinition, GeoShape.class)) {
                    throw new VertexiumException("GeoCompare query is only allowed for GeoShape types. Property " + propertyDefinition.getPropertyName() + " is not a GeoShape.");
                }
            });
        }

        @Override
        protected boolean isMatch(ExtendedDataRow extendedDataRow) {
            for (Property property : extendedDataRow.getProperties()) {
                if (this.keys.contains(property.getName())) {
                    PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(this.propertyDefinitions, property.getName());
                    Object columnValue = extendedDataRow.getPropertyValue(property.getName());
                    if (this.predicate.evaluate(columnValue, this.value, propertyDefinition)) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        protected boolean isMatch(Element element) {
            for (String key : this.keys) {
                if (this.predicate.evaluate(element.getProperties(key), this.value, this.propertyDefinitions)) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "predicate=" + predicate +
                    ", value=" + value +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
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
        private Set<String> keys;

        public HasPropertyContainer(String key) {
            this.keys = Collections.singleton(key);
        }

        public HasPropertyContainer(Iterable<String> keys) {
            this.keys = IterableUtils.toSet(keys);
        }

        public HasPropertyContainer(Class dataType, Collection<PropertyDefinition> propertyDefinitions) {
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());

            if (this.keys.isEmpty()) {
                throw new VertexiumException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
        }

        @Override
        protected boolean isMatch(ExtendedDataRow row) {
            for (Property prop : row.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean isMatch(Element element) {
            for (Property prop : element.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
                    '}';
        }
    }

    public static class HasNotPropertyContainer extends HasContainerSplitElementExtendedDataRows {
        private Set<String> keys;

        public HasNotPropertyContainer(String key) {
            this.keys = Collections.singleton(key);
        }

        public HasNotPropertyContainer(Iterable<String> keys) {
            this.keys = IterableUtils.toSet(keys);
        }

        public HasNotPropertyContainer(Class dataType, Collection<PropertyDefinition> propertyDefinitions) {
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());

            if (this.keys.isEmpty()) {
                throw new VertexiumException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
        }

        @Override
        protected boolean isMatch(ExtendedDataRow row) {
            for (Property prop : row.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected boolean isMatch(Element element) {
            for (Property prop : element.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return false;
                }
            }
            return true;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
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

    protected FetchHints idFetchHintsToElementFetchHints(EnumSet<IdFetchHint> idFetchHints) {
        return idFetchHints.contains(IdFetchHint.INCLUDE_HIDDEN) ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL;
    }
}
