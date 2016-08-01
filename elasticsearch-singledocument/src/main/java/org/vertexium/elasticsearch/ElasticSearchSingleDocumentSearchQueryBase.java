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
import org.elasticsearch.common.joda.time.DateTime;
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
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.elasticsearch.utils.PagingIterable;
import org.vertexium.query.*;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoHash;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;
import org.vertexium.util.*;

import java.io.IOException;
import java.util.*;

public class ElasticSearchSingleDocumentSearchQueryBase extends QueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation,
        GraphQueryWithStatisticsAggregation {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchSingleDocumentSearchQueryBase.class);
    public static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Query.class);
    private final Client client;
    private final boolean evaluateHasContainers;
    private final boolean evaluateQueryString;
    private final boolean evaluateSortContainers;
    private final StandardAnalyzer analyzer;
    private final ScoringStrategy scoringStrategy;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final int pageSize;

    public ElasticSearchSingleDocumentSearchQueryBase(
            Client client,
            Graph graph,
            String queryString,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            Authorizations authorizations
    ) {
        super(graph, queryString, authorizations);
        this.client = client;
        this.evaluateQueryString = false;
        this.evaluateHasContainers = true;
        this.evaluateSortContainers = false;
        this.pageSize = pageSize;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.indexSelectionStrategy = indexSelectionStrategy;
    }

    public ElasticSearchSingleDocumentSearchQueryBase(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            Authorizations authorizations
    ) {
        super(graph, similarToFields, similarToText, authorizations);
        this.client = client;
        this.evaluateQueryString = false;
        this.evaluateHasContainers = true;
        this.evaluateSortContainers = false;
        this.pageSize = pageSize;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.indexSelectionStrategy = indexSelectionStrategy;
    }

    @Override
    @Deprecated
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval, Long minDocumentCount) {
        addAggregation(new HistogramAggregation(aggregationName, fieldName, interval, minDocumentCount));
        return this;
    }

    @Override
    @Deprecated
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval) {
        return addHistogramAggregation(aggregationName, fieldName, interval, null);
    }

    @Override
    @Deprecated
    public GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String fieldName) {
        addAggregation(new TermsAggregation(aggregationName, fieldName));
        return this;
    }

    @Override
    @Deprecated
    public GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String fieldName, int precision) {
        addAggregation(new GeohashAggregation(aggregationName, fieldName, precision));
        return this;
    }

    @Override
    @Deprecated
    public GraphQueryWithStatisticsAggregation addStatisticsAggregation(String aggregationName, String field) {
        addAggregation(new StatisticsAggregation(aggregationName, field));
        return this;
    }

    @Override
    public boolean isAggregationSupported(Aggregation agg) {
        if (agg instanceof HistogramAggregation) {
            return true;
        }
        if (agg instanceof TermsAggregation) {
            return true;
        }
        if (agg instanceof GeohashAggregation) {
            return true;
        }
        if (agg instanceof StatisticsAggregation) {
            return true;
        }
        if (agg instanceof CalendarFieldAggregation) {
            return true;
        }
        return false;
    }

    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType, int skip, int limit, boolean includeAggregations) {
        AndFilterBuilder filterBuilder = getFilterBuilder(filters);
        String[] indicesToQuery = getIndexSelectionStrategy().getIndicesToQuery(this, elementType);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("indicesToQuery: %s", Joiner.on(", ").join(indicesToQuery));
        }
        SearchRequestBuilder searchRequestBuilder = getClient()
                .prepareSearch(indicesToQuery)
                .setTypes(ElasticsearchSingleDocumentSearchIndex.ELEMENT_TYPE)
                .setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder))
                .addField(ElasticsearchSingleDocumentSearchIndex.ELEMENT_TYPE_FIELD_NAME)
                .setFrom(skip)
                .setSize(limit);
        if (includeAggregations) {
            List<AbstractAggregationBuilder> aggs = getElasticsearchAggregations(getAggregations());
            for (AbstractAggregationBuilder aggregationBuilder : aggs) {
                searchRequestBuilder.addAggregation(aggregationBuilder);
            }
        }
        return searchRequestBuilder;
    }

    protected QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters) {
        String queryString = queryParameters.getQueryString();
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }
        ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
        Collection<String> fields = es.getQueryablePropertyNames(getGraph(), false, getParameters().getAuthorizations());
        QueryStringQueryBuilder qs = QueryBuilders.queryString(queryString);
        for (String field : fields) {
            qs = qs.field(field);
        }
        return qs;
    }

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
            filters.add(FilterBuilders.inFilter(ElasticsearchSingleDocumentSearchIndex.EDGE_LABEL_FIELD_NAME, edgeLabelsArray));
        }

        if (getParameters() instanceof QueryStringQueryParameters) {
            String queryString = ((QueryStringQueryParameters) getParameters()).getQueryString();
            if (queryString == null || queryString.equals("*")) {
                ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
                Collection<String> fields = es.getQueryableElementTypeVisibilityPropertyNames(getGraph(), getParameters().getAuthorizations());
                OrFilterBuilder atLeastOneFieldExistsFilter = new OrFilterBuilder();
                for (String field : fields) {
                    atLeastOneFieldExistsFilter.add(new ExistsFilterBuilder(field));
                }
                filters.add(atLeastOneFieldExistsFilter);
            }
        }
        return filters;
    }

    protected void applySort(SearchRequestBuilder q) {
        for (SortContainer sortContainer : getParameters().getSortContainers()) {
            SortOrder esOrder = sortContainer.direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
            if (Element.ID_PROPERTY_NAME.equals(sortContainer.propertyName)) {
                q.addSort("_uid", esOrder);
            } else if (Edge.LABEL_PROPERTY_NAME.equals(sortContainer.propertyName)) {
                q.addSort(ElasticsearchSingleDocumentSearchIndex.EDGE_LABEL_FIELD_NAME, esOrder);
            } else {
                PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(sortContainer.propertyName);
                if (propertyDefinition == null) {
                    continue;
                }
                if (!getSearchIndex().isPropertyInIndex(getGraph(), sortContainer.propertyName)) {
                    continue;
                }
                if (!propertyDefinition.isSortable()) {
                    throw new VertexiumException("Cannot sort on non-sortable fields");
                }
                q.addSort(propertyDefinition.getPropertyName() + ElasticsearchSingleDocumentSearchIndex.SORT_PROPERTY_NAME_SUFFIX, esOrder);
            }
        }
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
                    SearchHitField elementType = hit.getFields().get(ElasticsearchSingleDocumentSearchIndex.ELEMENT_TYPE_FIELD_NAME);
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
        return new EmptyElasticSearchGraphQueryIterable<>(ElasticSearchSingleDocumentSearchQueryBase.this, getParameters());
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
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("searching for: " + toString());
        }
        List<FilterBuilder> filters = getFilters(elementType);
        QueryBuilder query = createQuery(getParameters());
        query = scoringStrategy.updateQuery(query);
        SearchRequestBuilder q = getSearchRequestBuilder(filters, query, elementType, skip, limit, includeAggregations);
        applySort(q);

        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("query: %s", q);
        }
        return q.execute()
                .actionGet();
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
                filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(propertyName + ElasticsearchSingleDocumentSearchIndex.GEO_PROPERTY_NAME_SUFFIX)));
            } else if (isExactMatchPropertyDefinition(propDef)) {
                filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(propertyName + ElasticsearchSingleDocumentSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX)));
            }
        }
        return getSingleFilterOrAndTheFilters(filters, hasNotProperty);
    }

    protected FilterBuilder getFilterForHasPropertyContainer(HasPropertyContainer hasProperty) {
        String[] propertyNames = getPropertyNames(hasProperty.getKey());
        if (propertyNames.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(hasProperty.getKey());
        }
        PropertyDefinition propDef = getPropertyDefinition(hasProperty.getKey());
        if (propDef == null) {
            throw new VertexiumException("Could not find property definition for property name: " + hasProperty.getKey());
        }
        List<FilterBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(FilterBuilders.existsFilter(propertyName));
            if (propDef.getDataType().equals(GeoPoint.class)) {
                filters.add(FilterBuilders.existsFilter(propertyName + ElasticsearchSingleDocumentSearchIndex.GEO_PROPERTY_NAME_SUFFIX));
            } else if (isExactMatchPropertyDefinition(propDef)) {
                filters.add(FilterBuilders.existsFilter(propertyName + ElasticsearchSingleDocumentSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX));
            }
        }
        return getSingleFilterOrOrTheFilters(filters, hasProperty);
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
        if (keys.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            String propertyName = key + ElasticsearchSingleDocumentSearchIndex.GEO_PROPERTY_NAME_SUFFIX;
            switch (compare) {
                case WITHIN:
                    Object value = has.value;
                    if (value instanceof GeoHash) {
                        value = ((GeoHash) value).toGeoRect();
                    }

                    if (value instanceof GeoCircle) {
                        GeoCircle geoCircle = (GeoCircle) value;
                        double lat = geoCircle.getLatitude();
                        double lon = geoCircle.getLongitude();
                        double distance = geoCircle.getRadius();

                        String inflatedPropertyName = getSearchIndex().inflatePropertyName(propertyName);
                        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(inflatedPropertyName);
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
                    } else if (value instanceof GeoRect) {
                        GeoRect geoRect = (GeoRect) value;
                        double nwLat = geoRect.getNorthWest().getLatitude();
                        double nwLon = geoRect.getNorthWest().getLongitude();
                        double seLat = geoRect.getSouthEast().getLatitude();
                        double seLon = geoRect.getSouthEast().getLongitude();

                        String inflatedPropertyName = getSearchIndex().inflatePropertyName(propertyName);
                        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(inflatedPropertyName);
                        if (propertyDefinition != null && propertyDefinition.getDataType() == GeoCircle.class) {
                            ShapeBuilder shapeBuilder = ShapeBuilder.newPolygon()
                                    .point(nwLon, nwLat)
                                    .point(seLon, nwLat)
                                    .point(seLon, seLat)
                                    .point(nwLon, seLat)
                                    .close();
                            filters
                                    .add(new GeoShapeFilterBuilder(propertyName, shapeBuilder));
                        } else {
                            filters
                                    .add(FilterBuilders
                                            .geoBoundingBoxFilter(propertyName)
                                            .topLeft(nwLat, nwLon)
                                            .bottomRight(seLat, seLon));
                        }
                    } else {
                        throw new VertexiumException("Unexpected has value type " + value.getClass().getName());
                    }
                    break;
                default:
                    throw new VertexiumException("Unexpected GeoCompare predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    private FilterBuilder getSingleFilterOrOrTheFilters(List<FilterBuilder> filters, HasContainer has) {
        if (filters.size() > 1) {
            return FilterBuilders.orFilter(filters.toArray(new FilterBuilder[filters.size()]));
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter for: " + has);
        }
    }

    private FilterBuilder getSingleFilterOrAndTheFilters(List<FilterBuilder> filters, HasContainer has) {
        if (filters.size() > 1) {
            return FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter for: " + has);
        }
    }

    protected FilterBuilder getFilterForTextPredicate(TextPredicate compare, HasValueContainer has) {
        Object value = has.value;
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
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
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    protected FilterBuilder getFilterForContainsPredicate(Contains contains, HasValueContainer has) {
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            if (contains.equals(Contains.NOT_IN)) {
                return FilterBuilders.matchAllFilter();
            }
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<FilterBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (has.value instanceof Iterable) {
                has.value = IterableUtils.toArray((Iterable<?>) has.value, Object.class);
            }
            if (has.value instanceof String
                    || has.value instanceof String[]
                    || (has.value instanceof Object[] && ((Object[]) has.value).length > 0 && ((Object[]) has.value)[0] instanceof String)
                    ) {
                key = key + ElasticsearchSingleDocumentSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
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
        return getSingleFilterOrOrTheFilters(filters, has);
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
                key = key + ElasticsearchSingleDocumentSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
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
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    protected String[] getPropertyNames(String propertyName) {
        return getSearchIndex().getAllMatchingPropertyNames(getGraph(), propertyName, getParameters().getAuthorizations());
    }

    protected ElasticsearchSingleDocumentSearchIndex getSearchIndex() {
        return (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
    }

    protected void addElementTypeFilter(List<FilterBuilder> filters, ElasticSearchElementType elementType) {
        if (elementType != null) {
            filters.add(createElementTypeFilter(elementType));
        }
    }

    protected TermsFilterBuilder createElementTypeFilter(ElasticSearchElementType elementType) {
        return FilterBuilders.inFilter(ElasticsearchSingleDocumentSearchIndex.ELEMENT_TYPE_FIELD_NAME, elementType.getKey());
    }

    protected void addNotFilter(List<FilterBuilder> filters, String key, Object value) {
        filters.add(FilterBuilders.notFilter(FilterBuilders.inFilter(key, value)));
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

    protected QueryBuilder createQuery(QueryParameters queryParameters) {
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
            } else if (agg instanceof CalendarFieldAggregation) {
                aggs.addAll(getElasticsearchCalendarFieldAggregation((CalendarFieldAggregation) agg));
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
            geoHashAgg.field(propertyName + ElasticsearchSingleDocumentSearchIndex.GEO_PROPERTY_NAME_SUFFIX);
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
        if (Edge.LABEL_PROPERTY_NAME.equals(fieldName)) {
            TermsBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), "0"));
            termsAgg.field(fieldName);
            termsAggs.add(termsAgg);
        } else {
            PropertyDefinition propertyDefinition = getPropertyDefinition(fieldName);
            for (String propertyName : getPropertyNames(fieldName)) {
                if (isExactMatchPropertyDefinition(propertyDefinition)) {
                    propertyName = propertyName + ElasticsearchSingleDocumentSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                }

                String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
                TermsBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), visibilityHash));
                termsAgg.field(propertyName);

                for (AbstractAggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    termsAgg.subAggregation(subAgg);
                }

                termsAggs.add(termsAgg);
            }
        }
        return termsAggs;
    }

    private boolean isExactMatchPropertyDefinition(PropertyDefinition propertyDefinition) {
        return propertyDefinition != null
                && propertyDefinition.getDataType().equals(String.class)
                && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH);
    }

    private Collection<? extends AbstractAggregationBuilder> getElasticsearchCalendarFieldAggregation(CalendarFieldAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getPropertyDefinition(agg.getPropertyName());
        if (propertyDefinition == null) {
            throw new VertexiumException("Could not find mapping for property: " + agg.getPropertyName());
        }
        Class propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getPropertyNames(agg.getPropertyName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromDeflatedPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (propertyDataType == Date.class) {
                HistogramBuilder histAgg = AggregationBuilders.histogram(aggName);
                histAgg.interval(1);
                if (agg.getMinDocumentCount() != null) {
                    histAgg.minDocCount(agg.getMinDocumentCount());
                }
                String script = getCalendarFieldAggregationScript(agg, propertyName);
                histAgg.script(script);

                for (AbstractAggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    histAgg.subAggregation(subAgg);
                }

                aggs.add(histAgg);
            } else {
                throw new VertexiumException("Only dates are supported for hour of day aggregations");
            }
        }
        return aggs;
    }

    private String getCalendarFieldAggregationScript(CalendarFieldAggregation agg, String propertyName) {
        String prefix = "d = doc['" + propertyName + "']; ";
        switch (agg.getCalendarField()) {
            case Calendar.DAY_OF_MONTH:
                return prefix + "d ? d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).get(DateTimeFieldType.dayOfMonth()) : -1";
            case Calendar.DAY_OF_WEEK:
                return prefix + "d = (d ? (d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).get(DateTimeFieldType.dayOfWeek()) + 1) : -1); return d > 7 ? d - 7 : d;";
            case Calendar.HOUR_OF_DAY:
                return prefix + "d ? d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).get(DateTimeFieldType.hourOfDay()) : -1";
            case Calendar.MONTH:
                return prefix + "d ? (d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).get(DateTimeFieldType.monthOfYear()) - 1) : -1";
            case Calendar.YEAR:
                return prefix + "d ? d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).get(DateTimeFieldType.year()) : -1";
            default:
                LOGGER.warn("Slow operation toGregorianCalendar() for calendar field: %d", agg.getCalendarField());
                return prefix + "d ? d.date.toDateTime(DateTimeZone.forID(\"" + agg.getTimeZone().getID() + "\")).toGregorianCalendar().get(" + agg.getCalendarField() + ") : -1";
        }
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
                if (agg.getExtendedBounds() != null) {
                    HistogramAggregation.ExtendedBounds<?> bounds = agg.getExtendedBounds();
                    if (bounds.getMinMaxType().isAssignableFrom(Long.class)) {
                        dateAgg.extendedBounds((Long) bounds.getMin(), (Long) bounds.getMax());
                    } else if (bounds.getMinMaxType().isAssignableFrom(Date.class)) {
                        dateAgg.extendedBounds(new DateTime(bounds.getMin()), new DateTime(bounds.getMax()));
                    } else if (bounds.getMinMaxType().isAssignableFrom(String.class)) {
                        dateAgg.extendedBounds((String) bounds.getMin(), (String) bounds.getMax());
                    } else {
                        throw new VertexiumException("Unhandled extended bounds type. Expected Long, String, or Date. Found: " + bounds.getMinMaxType().getName());
                    }
                }

                for (AbstractAggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    dateAgg.subAggregation(subAgg);
                }

                aggs.add(dateAgg);
            } else {
                HistogramBuilder histogramAgg = AggregationBuilders.histogram(aggName);
                histogramAgg.field(propertyName);
                histogramAgg.interval(Long.parseLong(agg.getInterval()));
                if (agg.getMinDocumentCount() != null) {
                    histogramAgg.minDocCount(agg.getMinDocumentCount());
                }
                if (agg.getExtendedBounds() != null) {
                    HistogramAggregation.ExtendedBounds<?> bounds = agg.getExtendedBounds();
                    if (bounds.getMinMaxType().isAssignableFrom(Long.class)) {
                        histogramAgg.extendedBounds((Long) bounds.getMin(), (Long) bounds.getMax());
                    } else {
                        throw new VertexiumException("Unhandled extended bounds type. Expected Long. Found: " + bounds.getMinMaxType().getName());
                    }
                }

                for (AbstractAggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    histogramAgg.subAggregation(subAgg);
                }

                aggs.add(histogramAgg);
            }
        }
        return aggs;
    }

    protected PropertyDefinition getPropertyDefinition(String propertyName) {
        return getGraph().getPropertyDefinition(propertyName);
    }

    protected IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public String getAggregationName(String name) {
        return getSearchIndex().getAggregationName(name);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "parameters=" + getParameters() +
                ", evaluateHasContainers=" + evaluateHasContainers +
                ", evaluateQueryString=" + evaluateQueryString +
                ", evaluateSortContainers=" + evaluateSortContainers +
                ", pageSize=" + pageSize +
                '}';
    }
}

