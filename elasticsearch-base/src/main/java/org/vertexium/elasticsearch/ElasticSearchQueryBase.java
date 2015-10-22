package org.vertexium.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.elasticsearch.utils.PagingIterable;
import org.vertexium.query.*;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoPoint;
import org.vertexium.util.*;

import java.io.IOException;
import java.util.*;

public abstract class ElasticSearchQueryBase extends QueryBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchQueryBase.class);
    public static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Query.class);
    private final Client client;
    private final boolean evaluateHasContainers;
    private final boolean evaluateQueryString;
    private final boolean evaluateSortContainers;
    private final StandardAnalyzer analyzer;
    private final ScoringStrategy scoringStrategy;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final int pageSize;

    protected ElasticSearchQueryBase(
            Client client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            int pageSize,
            Authorizations authorizations
    ) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.client = client;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        this.evaluateSortContainers = evaluateSortContainers;
        this.pageSize = pageSize;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.indexSelectionStrategy = indexSelectionStrategy;
    }

    protected ElasticSearchQueryBase(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            int pageSize,
            Authorizations authorizations
    ) {
        super(graph, similarToFields, similarToText, propertyDefinitions, authorizations);
        this.client = client;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        this.evaluateSortContainers = evaluateSortContainers;
        this.pageSize = pageSize;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.indexSelectionStrategy = indexSelectionStrategy;
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(final EnumSet<FetchHint> fetchHints) {
        return new PagingIterable<Vertex>(getParameters().getSkip(), getParameters().getLimit(), pageSize) {
            @Override
            protected ElasticSearchGraphQueryIterable<Vertex> getPageIterable(int skip, int limit, boolean includeAggregations) {
                long startTime = System.nanoTime();
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticSearchElementType.VERTEX, skip, limit, includeAggregations);
                } catch (IndexMissingException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (VertexiumNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }
                final SearchHits hits = response.getHits();
                List<String> ids = IterableUtils.toList(new ConvertingIterable<SearchHit, String>(hits) {
                    @Override
                    protected String convert(SearchHit searchHit) {
                        return searchHit.getId();
                    }
                });
                long endTime = System.nanoTime();
                long searchTime = endTime - startTime;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("elastic search results %d of %d (time: %dms)", ids.size(), hits.getTotalHits(), searchTime / 1000 / 1000);
                }

                // since ES doesn't support security we will rely on the graph to provide vertex filtering
                // and rely on the DefaultGraphQueryIterable to provide property filtering
                QueryParameters filterParameters = getParameters().clone();
                filterParameters.setSkip(0); // ES already did a skip
                Iterable<Vertex> vertices = getGraph().getVertices(ids, fetchHints, filterParameters.getAuthorizations());
                vertices = sortByResultOrder(vertices, ids);
                return createIterable(response, filterParameters, vertices, evaluateQueryString, evaluateHasContainers, evaluateSortContainers, searchTime, hits);
            }
        };
    }

    @Override
    public QueryResultsIterable<Edge> edges(final EnumSet<FetchHint> fetchHints) {
        return new PagingIterable<Edge>(getParameters().getSkip(), getParameters().getLimit(), pageSize) {
            @Override
            protected ElasticSearchGraphQueryIterable<Edge> getPageIterable(int skip, int limit, boolean includeAggregations) {
                long startTime = System.nanoTime();
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticSearchElementType.EDGE, skip, limit, includeAggregations);
                } catch (IndexMissingException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (VertexiumNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }
                final SearchHits hits = response.getHits();
                List<String> ids = IterableUtils.toList(new ConvertingIterable<SearchHit, String>(hits) {
                    @Override
                    protected String convert(SearchHit searchHit) {
                        return searchHit.getId();
                    }
                });
                long endTime = System.nanoTime();
                long searchTime = endTime - startTime;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("elastic search results %d of %d (time: %dms)", ids.size(), hits.getTotalHits(), (endTime - startTime) / 1000 / 1000);
                }

                // since ES doesn't support security we will rely on the graph to provide edge filtering
                // and rely on the DefaultGraphQueryIterable to provide property filtering
                QueryParameters filterParameters = getParameters().clone();
                filterParameters.setSkip(0); // ES already did a skip
                Iterable<Edge> edges = getGraph().getEdges(ids, fetchHints, filterParameters.getAuthorizations());
                edges = sortByResultOrder(edges, ids);
                // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
                return createIterable(response, filterParameters, edges, evaluateQueryString, evaluateHasContainers, evaluateSortContainers, searchTime, hits);
            }
        };
    }

    @Override
    public QueryResultsIterable<Element> elements(final EnumSet<FetchHint> fetchHints) {
        return new PagingIterable<Element>(getParameters().getSkip(), getParameters().getLimit(), pageSize) {
            @Override
            protected ElasticSearchGraphQueryIterable<Element> getPageIterable(int skip, int limit, boolean includeAggregations) {
                long startTime = System.nanoTime();
                SearchResponse response;
                try {
                    response = getSearchResponse(null, skip, limit, includeAggregations);
                } catch (IndexMissingException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (VertexiumNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }
                final SearchHits hits = response.getHits();
                List<String> vertexIds = new ArrayList<>();
                List<String> edgeIds = new ArrayList<>();
                List<String> ids = new ArrayList<>();
                for (SearchHit hit : hits) {
                    SearchHitField elementType = hit.getFields().get(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME);
                    if (elementType == null) {
                        continue;
                    }
                    ElasticSearchElementType et = ElasticSearchElementType.parse(elementType.getValue().toString());
                    ids.add(hit.getId());
                    switch (et) {
                        case VERTEX:
                            vertexIds.add(hit.getId());
                            break;
                        case EDGE:
                            edgeIds.add(hit.getId());
                            break;
                        default:
                            LOGGER.warn("Unhandled element type returned: %s", elementType);
                            break;
                    }
                }
                long endTime = System.nanoTime();
                long searchTime = endTime - startTime;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "elastic search results (vertices: %d, edges: %d = %d) of %d (time: %dms)",
                            vertexIds.size(),
                            edgeIds.size(),
                            vertexIds.size() + edgeIds.size(),
                            hits.getTotalHits(),
                            (endTime - startTime) / 1000 / 1000);
                }

                // since ES doesn't support security we will rely on the graph to provide edge filtering
                // and rely on the DefaultGraphQueryIterable to provide property filtering
                QueryParameters filterParameters = getParameters().clone();
                filterParameters.setSkip(0); // ES already did a skip
                Iterable<Element> vertices = IterableUtils.toElementIterable(getGraph().getVertices(vertexIds, fetchHints, filterParameters.getAuthorizations()));
                Iterable<Element> edges = IterableUtils.toElementIterable(getGraph().getEdges(edgeIds, fetchHints, filterParameters.getAuthorizations()));
                Iterable<Element> elements = new JoinIterable<>(vertices, edges);
                elements = sortByResultOrder(elements, ids);
                // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
                return createIterable(response, filterParameters, elements, evaluateQueryString, evaluateHasContainers, evaluateSortContainers, searchTime, hits);
            }
        };
    }

    private <T extends Element> Iterable<T> sortByResultOrder(Iterable<T> elements, List<String> ids) {
        ImmutableMap<String, T> elementsMap = Maps.uniqueIndex(elements, new Function<T, String>() {
            @Override
            public String apply(T e) {
                return e.getId();
            }
        });

        List<T> results = new ArrayList<>();
        for (String id : ids) {
            T element = elementsMap.get(id);
            if (element != null) {
                results.add(element);
            }
        }
        return results;
    }

    private <T extends Element> EmptyElasticSearchGraphQueryIterable<T> createEmptyIterable() {
        return new EmptyElasticSearchGraphQueryIterable<>(ElasticSearchQueryBase.this, getParameters());
    }

    protected <T extends Element> ElasticSearchGraphQueryIterable<T> createIterable(
            SearchResponse response,
            QueryParameters filterParameters,
            Iterable<T> elements,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            long searchTime,
            SearchHits hits
    ) {
        return new ElasticSearchGraphQueryIterable<>(
                this,
                response,
                filterParameters,
                elements,
                evaluateQueryString,
                evaluateHasContainers,
                evaluateSortContainers,
                hits.getTotalHits(),
                searchTime,
                hits
        );
    }

    private SearchResponse getSearchResponse(ElasticSearchElementType elementType, int skip, int limit, boolean includeAggregations) {
        List<FilterBuilder> filters = getFilters(elementType);
        QueryBuilder query = createQuery(getParameters(), elementType, filters);
        query = scoringStrategy.updateQuery(query);
        SearchRequestBuilder q = getSearchRequestBuilder(filters, query, elementType, skip, limit, includeAggregations);
        applySort(q);

        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("query: %s", q);
        }
        return q.execute()
                .actionGet();
    }

    protected abstract void applySort(SearchRequestBuilder q);

    protected List<FilterBuilder> getFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> filters = new ArrayList<>();
        if (elementType != null) {
            addElementTypeFilter(filters, elementType);
        }
        for (HasContainer has : getParameters().getHasContainers()) {
            if (has instanceof HasValueContainer) {
                filters.add(getFiltersForHasValueContainer((HasValueContainer) has));
            } else if (has instanceof HasPropertyContainer) {
                filters.add(getFilterForHasPropertyContainer((HasPropertyContainer) has));
            } else if (has instanceof HasNotPropertyContainer) {
                filters.add(getFilterForHasNotPropertyContainer((HasNotPropertyContainer) has));
            } else {
                throw new VertexiumException("Unexpected type " + has.getClass().getName());
            }
        }
        if (getParameters().getEdgeLabels().size() > 0) {
            String[] edgeLabelsArray = getParameters().getEdgeLabels().toArray(new String[getParameters().getEdgeLabels().size()]);
            filters.add(FilterBuilders.inFilter(ElasticSearchSearchIndexBase.EDGE_LABEL_FIELD_NAME, edgeLabelsArray));
        }
        return filters;
    }

    protected FilterBuilder getFilterForHasNotPropertyContainer(HasNotPropertyContainer hasNotProperty) {
        String[] propertyNames;
        try {
            propertyNames = getPropertyNames(hasNotProperty.getKey());
            if (propertyNames.length == 0) {
                throw new VertexiumNoMatchingPropertiesException(hasNotProperty.getKey());
            }
        } catch (VertexiumNoMatchingPropertiesException ex) {
            // If we can't find a property this means it doesn't exist on any elements so the hasNot query should
            // match all records.
            return FilterBuilders.matchAllFilter();
        }
        PropertyDefinition propDef = getPropertyDefinition(hasNotProperty.getKey());
        List<FilterBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(propertyName)));
            if (propDef.getDataType().equals(GeoPoint.class)) {
                filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(propertyName + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX)));
            } else if (propDef.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(propertyName + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX)));
            }
        }
        return getSingleFilterOrAndTheFilters(filters);
    }

    protected FilterBuilder getFilterForHasPropertyContainer(HasPropertyContainer hasProperty) {
        String[] propertyNames = getPropertyNames(hasProperty.getKey());
        if (propertyNames.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(hasProperty.getKey());
        }
        PropertyDefinition propDef = getPropertyDefinition(hasProperty.getKey());
        List<FilterBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(FilterBuilders.existsFilter(propertyName));
            if (propDef.getDataType().equals(GeoPoint.class)) {
                filters.add(FilterBuilders.existsFilter(propertyName + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX));
            } else if (propDef.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                filters.add(FilterBuilders.existsFilter(propertyName + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX));
            }
        }
        return getSingleFilterOrOrTheFilters(filters);
    }

    protected FilterBuilder getFiltersForHasValueContainer(HasValueContainer has) {
        if (has.predicate instanceof Compare) {
            return getFilterForComparePredicate((Compare) has.predicate, has);
        } else if (has.predicate instanceof Contains) {
            return getFilterForContainsPredicate((Contains) has.predicate, has);
        } else if (has.predicate instanceof TextPredicate) {
            return getFilterForTextPredicate((TextPredicate) has.predicate, has);
        } else if (has.predicate instanceof GeoCompare) {
            return getFilterForGeoComparePredicate((GeoCompare) has.predicate, has);
        } else {
            throw new VertexiumException("Unexpected predicate type " + has.predicate.getClass().getName());
        }
    }

    protected FilterBuilder getFilterForGeoComparePredicate(GeoCompare compare, HasValueContainer has) {
        String[] keys = getPropertyNames(has.key);
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            String propertyName = key + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX;
            switch (compare) {
                case WITHIN:
                    if (has.value instanceof GeoCircle) {
                        GeoCircle geoCircle = (GeoCircle) has.value;
                        double lat = geoCircle.getLatitude();
                        double lon = geoCircle.getLongitude();
                        double distance = geoCircle.getRadius();

                        PropertyDefinition propertyDefinition = this.getPropertyDefinitions().get(propertyName);
                        if (propertyDefinition != null && propertyDefinition.getDataType() == GeoCircle.class) {
                            ShapeBuilder shapeBuilder = ShapeBuilder.newCircleBuilder()
                                    .center(lon, lat)
                                    .radius(distance, DistanceUnit.KILOMETERS);
                            filters
                                    .add(new GeoShapeFilterBuilder(propertyName, shapeBuilder));
                        } else {
                            filters
                                    .add(FilterBuilders
                                            .geoDistanceFilter(propertyName)
                                            .point(lat, lon)
                                            .distance(distance, DistanceUnit.KILOMETERS));
                        }
                    } else {
                        throw new VertexiumException("Unexpected has value type " + has.value.getClass().getName());
                    }
                    break;
                default:
                    throw new VertexiumException("Unexpected GeoCompare predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters);
    }

    private FilterBuilder getSingleFilterOrOrTheFilters(List<FilterBuilder> filters) {
        if (filters.size() > 1) {
            return FilterBuilders.orFilter(filters.toArray(new FilterBuilder[filters.size()]));
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter");
        }
    }

    private FilterBuilder getSingleFilterOrAndTheFilters(List<FilterBuilder> filters) {
        if (filters.size() > 1) {
            return FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter");
        }
    }

    protected FilterBuilder getFilterForTextPredicate(TextPredicate compare, HasValueContainer has) {
        Object value = has.value;
        String[] keys = getPropertyNames(has.key);
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (value instanceof String) {
                value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
            }
            switch (compare) {
                case CONTAINS:
                    if (value instanceof String) {
                        filters.add(FilterBuilders.termsFilter(key, splitStringIntoTerms((String) value)).execution("and"));
                    } else {
                        filters.add(FilterBuilders.termFilter(key, value));
                    }
                    break;
                default:
                    throw new VertexiumException("Unexpected text predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters);
    }

    protected FilterBuilder getFilterForContainsPredicate(Contains contains, HasValueContainer has) {
        String[] keys = getPropertyNames(has.key);
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (has.value instanceof Iterable) {
                has.value = IterableUtils.toArray((Iterable<?>) has.value, Object.class);
            }
            if (has.value instanceof String
                    || has.value instanceof String[]
                    || (has.value instanceof Object[] && ((Object[]) has.value).length > 0 && ((Object[]) has.value)[0] instanceof String)
                    ) {
                key = key + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }
            switch (contains) {
                case IN:
                    filters.add(FilterBuilders.inFilter(key, (Object[]) has.value));
                    break;
                case NOT_IN:
                    filters.add(FilterBuilders.notFilter(FilterBuilders.inFilter(key, (Object[]) has.value)));
                    break;
                default:
                    throw new VertexiumException("Unexpected Contains predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters);
    }

    protected FilterBuilder getFilterForComparePredicate(Compare compare, HasValueContainer has) {
        Object value = has.value;
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            if (compare.equals(Compare.NOT_EQUAL)) {
                return FilterBuilders.matchAllFilter();
            }
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (value instanceof String || value instanceof String[]) {
                key = key + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }
            switch (compare) {
                case EQUAL:
                    if (value instanceof DateOnly) {
                        DateOnly dateOnlyValue = ((DateOnly) value);
                        filters.add(FilterBuilders.rangeFilter(key).from(dateOnlyValue.toString()).to(dateOnlyValue.toString()));
                    } else {
                        filters.add(FilterBuilders.termFilter(key, value));
                    }
                    break;
                case GREATER_THAN_EQUAL:
                    filters.add(FilterBuilders.rangeFilter(key).gte(value));
                    break;
                case GREATER_THAN:
                    filters.add(FilterBuilders.rangeFilter(key).gt(value));
                    break;
                case LESS_THAN_EQUAL:
                    filters.add(FilterBuilders.rangeFilter(key).lte(value));
                    break;
                case LESS_THAN:
                    filters.add(FilterBuilders.rangeFilter(key).lt(value));
                    break;
                case NOT_EQUAL:
                    addNotFilter(filters, key, value);
                    break;
                default:
                    throw new VertexiumException("Unexpected Compare predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters);
    }

    protected String[] getPropertyNames(String propertyName) {
        return getSearchIndex().getAllMatchingPropertyNames(getGraph(), propertyName, getParameters().getAuthorizations());
    }

    protected ElasticSearchSearchIndexBase getSearchIndex() {
        return (ElasticSearchSearchIndexBase) ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex();
    }

    protected void addElementTypeFilter(List<FilterBuilder> filters, ElasticSearchElementType elementType) {
        if (elementType != null) {
            filters.add(createElementTypeFilter(elementType));
        }
    }

    protected TermsFilterBuilder createElementTypeFilter(ElasticSearchElementType elementType) {
        return FilterBuilders.inFilter(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME, elementType.getKey());
    }

    protected void addNotFilter(List<FilterBuilder> filters, String key, Object value) {
        filters.add(FilterBuilders.notFilter(FilterBuilders.inFilter(key, value)));
    }

    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType, int skip, int limit, boolean includeAggregations) {
        AndFilterBuilder filterBuilder = getFilterBuilder(filters);
        String[] indicesToQuery = getIndexSelectionStrategy().getIndicesToQuery(this, elementType);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("indicesToQuery: %s", Joiner.on(", ").join(indicesToQuery));
        }
        return getClient()
                .prepareSearch(indicesToQuery)
                .setTypes(ElasticSearchSearchIndexBase.ELEMENT_TYPE)
                .setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder))
                .addField(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME)
                .setFrom(skip)
                .setSize(limit);
    }

    protected AndFilterBuilder getFilterBuilder(List<FilterBuilder> filters) {
        return FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
    }

    private String[] splitStringIntoTerms(String value) {
        try {
            List<String> results = new ArrayList<>();
            try (TokenStream tokens = analyzer.tokenStream("", value)) {
                CharTermAttribute term = tokens.getAttribute(CharTermAttribute.class);
                tokens.reset();
                while (tokens.incrementToken()) {
                    String t = term.toString().trim();
                    if (t.length() > 0) {
                        results.add(t);
                    }
                }
            }
            return results.toArray(new String[results.size()]);
        } catch (IOException e) {
            throw new VertexiumException("Could not tokenize string: " + value, e);
        }
    }

    protected QueryBuilder createQuery(QueryParameters queryParameters, ElasticSearchElementType elementType, List<FilterBuilder> filters) {
        if (queryParameters instanceof QueryStringQueryParameters) {
            return createQueryStringQuery((QueryStringQueryParameters) queryParameters);
        } else if (queryParameters instanceof SimilarToTextQueryParameters) {
            return createSimilarToTextQuery((SimilarToTextQueryParameters) queryParameters);
        } else {
            throw new VertexiumException("Query parameters not supported of type: " + queryParameters.getClass().getName());
        }
    }

    protected QueryBuilder createSimilarToTextQuery(SimilarToTextQueryParameters queryParameters) {
        SimilarToTextQueryParameters similarTo = queryParameters;
        List<String> allFields = new ArrayList<>();
        String[] fields = similarTo.getFields();
        for (String field : fields) {
            Collections.addAll(allFields, getPropertyNames(field));
        }
        MoreLikeThisQueryBuilder q = QueryBuilders.moreLikeThisQuery(allFields.toArray(new String[allFields.size()]))
                .likeText(similarTo.getText());
        if (similarTo.getPercentTermsToMatch() != null) {
            q.percentTermsToMatch(similarTo.getPercentTermsToMatch());
        }
        if (similarTo.getMinTermFrequency() != null) {
            q.minTermFreq(similarTo.getMinTermFrequency());
        }
        if (similarTo.getMaxQueryTerms() != null) {
            q.maxQueryTerms(similarTo.getMaxQueryTerms());
        }
        if (similarTo.getMinDocFrequency() != null) {
            q.minDocFreq(similarTo.getMinDocFrequency());
        }
        if (similarTo.getMaxDocFrequency() != null) {
            q.maxDocFreq(similarTo.getMaxDocFrequency());
        }
        if (similarTo.getBoost() != null) {
            q.boost(similarTo.getBoost());
        }
        return q;
    }

    protected QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters) {
        String queryString = queryParameters.getQueryString();
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }
        return QueryBuilders.queryString(queryString);
    }

    public Client getClient() {
        return client;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchAggregations(Iterable<Aggregation> aggregations) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (Aggregation agg : aggregations) {
            if (agg instanceof HistogramAggregation) {
                aggs.addAll(getElasticsearchHistogramAggregations((HistogramAggregation) agg));
            } else if (agg instanceof TermsAggregation) {
                aggs.addAll(getElasticsearchTermsAggregations((TermsAggregation) agg));
            } else if (agg instanceof GeohashAggregation) {
                aggs.addAll(getElasticsearchGeohashAggregations((GeohashAggregation) agg));
            } else if (agg instanceof StatisticsAggregation) {
                aggs.addAll(getElasticsearchStatisticsAggregations((StatisticsAggregation) agg));
            } else {
                throw new VertexiumException("Could not add aggregation of type: " + agg.getClass().getName());
            }
        }
        return aggs;
    }

    protected List<AggregationBuilder> getElasticsearchGeohashAggregations(GeohashAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            GeoHashGridBuilder geoHashAgg = AggregationBuilders.geohashGrid(aggName);
            geoHashAgg.field(propertyName + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX);
            geoHashAgg.precision(agg.getPrecision());
            aggs.add(geoHashAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchStatisticsAggregations(StatisticsAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            ExtendedStatsBuilder statsAgg = AggregationBuilders.extendedStats(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    private String createAggregationName(String aggName, String visibilityHash) {
        if (visibilityHash != null && visibilityHash.length() > 0) {
            return aggName + "_" + visibilityHash;
        }
        return aggName;
    }

    protected List<AggregationBuilder> getElasticsearchTermsAggregations(TermsAggregation agg) {
        List<AggregationBuilder> termsAggs = new ArrayList<>();
        String fieldName = agg.getPropertyName();
        PropertyDefinition propertyDefinition = getPropertyDefinition(fieldName);
        for (String propertyName : getPropertyNames(fieldName)) {
            if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                propertyName = propertyName + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }

            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
            TermsBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), visibilityHash));
            termsAgg.field(propertyName);

            for (AbstractAggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                termsAgg.subAggregation(subAgg);
            }

            termsAggs.add(termsAgg);
        }
        return termsAggs;
    }

    protected List<AggregationBuilder> getElasticsearchHistogramAggregations(HistogramAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new VertexiumException("Could not find mapping for property: " + agg.getFieldName());
        }
        Class propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (propertyDataType == Date.class) {
                DateHistogramBuilder dateAgg = AggregationBuilders.dateHistogram(aggName);
                dateAgg.field(propertyName);
                dateAgg.interval(new DateHistogram.Interval(agg.getInterval()));
                if (agg.getMinDocumentCount() != null) {
                    dateAgg.minDocCount(agg.getMinDocumentCount());
                }
                aggs.add(dateAgg);
            } else {
                HistogramBuilder histogramAgg = AggregationBuilders.histogram(aggName);
                histogramAgg.field(propertyName);
                histogramAgg.interval(Long.parseLong(agg.getInterval()));
                if (agg.getMinDocumentCount() != null) {
                    histogramAgg.minDocCount(agg.getMinDocumentCount());
                }
                aggs.add(histogramAgg);
            }
        }
        return aggs;
    }

    private PropertyDefinition getPropertyDefinition(String propertyName) {
        return getSearchIndex().getPropertyDefinition(getGraph(), propertyName);
    }

    protected IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public String getAggregationName(String name) {
        return getSearchIndex().getAggregationName(name);
    }
}
