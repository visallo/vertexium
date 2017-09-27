package org.vertexium.elasticsearch5;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.vertexium.*;
import org.vertexium.elasticsearch5.score.ScoringStrategy;
import org.vertexium.elasticsearch5.utils.ElasticsearchExtendedDataIdUtils;
import org.vertexium.elasticsearch5.utils.InfiniteScrollIterable;
import org.vertexium.elasticsearch5.utils.PagingIterable;
import org.vertexium.query.*;
import org.vertexium.type.*;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.JoinIterable;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.FIELDNAME_DOT_REPLACEMENT;

public class ElasticsearchSearchQueryBase extends QueryBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchSearchQueryBase.class);
    public static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Query.class);
    private final Client client;
    private final boolean evaluateHasContainers;
    private final boolean evaluateQueryString;
    private final boolean evaluateSortContainers;
    private final StandardAnalyzer analyzer;
    private final ScoringStrategy scoringStrategy;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final int pageSize;
    private final int pagingLimit;
    private final TimeValue scrollKeepAlive;
    private final int termAggregationShardSize;

    public ElasticsearchSearchQueryBase(
            Client client,
            Graph graph,
            String queryString,
            Options options,
            Authorizations authorizations
    ) {
        super(graph, queryString, authorizations);
        this.client = client;
        this.evaluateQueryString = false;
        this.evaluateHasContainers = true;
        this.evaluateSortContainers = false;
        this.pageSize = options.pageSize;
        this.scoringStrategy = options.scoringStrategy;
        this.indexSelectionStrategy = options.indexSelectionStrategy;
        this.scrollKeepAlive = options.scrollKeepAlive;
        this.pagingLimit = options.pagingLimit;
        this.analyzer = options.analyzer;
        this.termAggregationShardSize = options.termAggregationShardSize;
    }

    public ElasticsearchSearchQueryBase(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Options options,
            Authorizations authorizations
    ) {
        super(graph, similarToFields, similarToText, authorizations);
        this.client = client;
        this.evaluateQueryString = false;
        this.evaluateHasContainers = true;
        this.evaluateSortContainers = false;
        this.pageSize = options.pageSize;
        this.scoringStrategy = options.scoringStrategy;
        this.indexSelectionStrategy = options.indexSelectionStrategy;
        this.scrollKeepAlive = options.scrollKeepAlive;
        this.pagingLimit = options.pagingLimit;
        this.analyzer = options.analyzer;
        this.termAggregationShardSize = options.termAggregationShardSize;
    }

    @Override
    public boolean isAggregationSupported(Aggregation agg) {
        if (agg instanceof HistogramAggregation) {
            return true;
        }
        if (agg instanceof RangeAggregation) {
            return true;
        }
        if (agg instanceof PercentilesAggregation) {
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

    private SearchRequestBuilder buildQuery(EnumSet<ElasticsearchDocumentType> elementType, boolean includeAggregations) {
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("searching for: " + toString());
        }
        List<QueryBuilder> filters = getFilters(elementType);
        QueryBuilder query = createQuery(getParameters());
        query = scoringStrategy.updateQuery(query);

        QueryBuilder filterBuilder = getFilterBuilder(filters);
        String[] indicesToQuery = getIndexSelectionStrategy().getIndicesToQuery(this, elementType);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("indicesToQuery: %s", Joiner.on(", ").join(indicesToQuery));
        }
        SearchRequestBuilder searchRequestBuilder = getClient()
                .prepareSearch(indicesToQuery)
                .setTypes(Elasticsearch5SearchIndex.ELEMENT_TYPE)
                .setQuery(QueryBuilders.boolQuery().must(query).filter(filterBuilder))
                .storedFields(
                        Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                        Elasticsearch5SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME,
                        Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME,
                        Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME
                );
        if (includeAggregations) {
            List<AggregationBuilder> aggs = getElasticsearchAggregations(getAggregations());
            for (AggregationBuilder aggregationBuilder : aggs) {
                searchRequestBuilder.addAggregation(aggregationBuilder);
            }
        }

        applySort(searchRequestBuilder);

        return searchRequestBuilder;
    }

    protected QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters) {
        String queryString = queryParameters.getQueryString();
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }
        Elasticsearch5SearchIndex es = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
        if (es.isServerPluginInstalled()) {
            return VertexiumQueryStringQueryBuilder.build(queryString, getParameters().getAuthorizations());
        } else {
            Collection<String> fields = es.getQueryablePropertyNames(getGraph(), getParameters().getAuthorizations());
            QueryStringQueryBuilder qs = QueryBuilders.queryStringQuery(queryString);
            for (String field : fields) {
                qs = qs.field(field);
            }
            return qs;
        }
    }

    protected List<QueryBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<QueryBuilder> filters = new ArrayList<>();
        if (elementTypes != null) {
            addElementTypeFilter(filters, elementTypes);
        }
        for (HasContainer has : getParameters().getHasContainers()) {
            if (has instanceof HasValueContainer) {
                filters.add(getFiltersForHasValueContainer((HasValueContainer) has));
            } else if (has instanceof HasPropertyContainer) {
                filters.add(getFilterForHasPropertyContainer((HasPropertyContainer) has));
            } else if (has instanceof HasNotPropertyContainer) {
                filters.add(getFilterForHasNotPropertyContainer((HasNotPropertyContainer) has));
            } else if (has instanceof HasExtendedData) {
                filters.add(getFilterForHasExtendedData((HasExtendedData) has));
            } else {
                throw new VertexiumException("Unexpected type " + has.getClass().getName());
            }
        }
        if ((elementTypes == null || elementTypes.contains(ElasticsearchDocumentType.EDGE))
                && getParameters().getEdgeLabels().size() > 0) {
            String[] edgeLabelsArray = getParameters().getEdgeLabels().toArray(new String[getParameters().getEdgeLabels().size()]);
            filters.add(QueryBuilders.termsQuery(Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME, edgeLabelsArray));
        }

        if (getParameters() instanceof QueryStringQueryParameters) {
            String queryString = ((QueryStringQueryParameters) getParameters()).getQueryString();
            if (queryString == null || queryString.equals("*")) {
                Elasticsearch5SearchIndex es = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
                Collection<String> fields = es.getQueryableElementTypeVisibilityPropertyNames(getGraph(), getParameters().getAuthorizations());
                BoolQueryBuilder atLeastOneFieldExistsFilter = QueryBuilders.boolQuery();
                for (String field : fields) {
                    atLeastOneFieldExistsFilter.should(new ExistsQueryBuilder(field));
                }
                atLeastOneFieldExistsFilter.minimumShouldMatch(1);
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
                q.addSort(Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME, esOrder);
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

                String[] propertyNames = getPropertyNames(propertyDefinition.getPropertyName());
                if (propertyNames.length > 1) {
                    String scriptSrc = "def fieldValues = []; for (def fieldName : params.fieldNames) { fieldValues.addAll(doc[fieldName].values); } " +
                            "if (params.esOrder == 'asc') { Collections.sort(fieldValues); } else { Collections.sort(fieldValues, Collections.reverseOrder()); }" +
                            "if (params.dataType == 'String') { return fieldValues; } else { return fieldValues.length > 0 ? fieldValues[0] : (params.esOrder == 'asc' ? Integer.MAX_VALUE : Integer.MIN_VALUE); }";

                    List<String> fieldNames = Arrays.stream(propertyNames).map(propertyName ->
                            propertyName + (propertyDefinition.getDataType() == String.class ? Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX : "")
                    ).collect(Collectors.toList());
                    HashMap<String, Object> scriptParams = new HashMap<>();
                    scriptParams.put("fieldNames", fieldNames);
                    scriptParams.put("esOrder", esOrder == SortOrder.DESC ? "desc" : "asc");
                    scriptParams.put("dataType", propertyDefinition.getDataType().getSimpleName());
                    Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptSrc, Collections.emptyMap(), scriptParams);
                    ScriptSortBuilder.ScriptSortType sortType = propertyDefinition.getDataType() == String.class ? ScriptSortBuilder.ScriptSortType.STRING : ScriptSortBuilder.ScriptSortType.NUMBER;
                    q.addSort(SortBuilders.scriptSort(script, sortType)
                            .order(esOrder)
                            .sortMode(esOrder == SortOrder.DESC ? SortMode.MAX : SortMode.MIN));
                } else {
                    String sortField = propertyNames[0];
                    if (propertyDefinition.getDataType() == String.class) {
                        sortField += Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                    }
                    q.addSort(sortField, esOrder);
                }
            }
        }
    }

    @Override
    public QueryResultsIterable<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, EnumSet<FetchHint> fetchHints) {
        if (shouldUseScrollApi()) {
            return searchScroll(objectTypes, fetchHints);
        }
        return searchPaged(objectTypes, fetchHints);
    }

    private QueryResultsIterable<? extends VertexiumObject> searchScroll(EnumSet<VertexiumObjectType> objectTypes, EnumSet<FetchHint> fetchHints) {
        return new QueryInfiniteScrollIterable<VertexiumObject>(objectTypes) {
            @Override
            protected ElasticsearchGraphQueryIterable<VertexiumObject> searchResponseToIterable(SearchResponse searchResponse) {
                return ElasticsearchSearchQueryBase.this.searchResponseToVertexiumObjectIterable(searchResponse, fetchHints);
            }
        };
    }

    private void closeScroll(String scrollId) {
        try {
            client.prepareClearScroll()
                    .addScrollId(scrollId)
                    .execute().actionGet();
        } catch (Exception ex) {
            throw new VertexiumException("Could not close iterator " + scrollId, ex);
        }
    }

    private QueryResultsIterable<? extends VertexiumObject> searchPaged(EnumSet<VertexiumObjectType> objectTypes, EnumSet<FetchHint> fetchHints) {
        return new PagingIterable<VertexiumObject>(getParameters().getSkip(), getParameters().getLimit(), pageSize) {
            @Override
            protected ElasticsearchGraphQueryIterable<VertexiumObject> getPageIterable(int skip, int limit, boolean includeAggregations) {
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticsearchDocumentType.fromVertexiumObjectTypes(objectTypes), skip, limit, includeAggregations);
                } catch (IndexNotFoundException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (VertexiumNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }
                return searchResponseToVertexiumObjectIterable(response, fetchHints);
            }
        };
    }

    private ElasticsearchGraphQueryIterable<VertexiumObject> searchResponseToVertexiumObjectIterable(SearchResponse response, EnumSet<FetchHint> fetchHints) {
        final SearchHits hits = response.getHits();
        Ids ids = new Ids(hits);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "elasticsearch results (vertices: %d + edges: %d + extended data: %d = %d)",
                    ids.getVertexIds().size(),
                    ids.getEdgeIds().size(),
                    ids.getExtendedDataIds().size(),
                    ids.getVertexIds().size() + ids.getEdgeIds().size() + ids.getExtendedDataIds().size()
            );
        }

        // since ES doesn't support security we will rely on the graph to provide edge filtering
        // and rely on the DefaultGraphQueryIterable to provide property filtering
        QueryParameters filterParameters = getParameters().clone();
        filterParameters.setSkip(0); // ES already did a skip
        List<Iterable<? extends VertexiumObject>> items = new ArrayList<>();
        if (ids.getVertexIds().size() > 0) {
            Iterable<? extends VertexiumObject> vertices = getGraph().getVertices(ids.getVertexIds(), fetchHints, filterParameters.getAuthorizations());
            items.add(vertices);
        }
        if (ids.getEdgeIds().size() > 0) {
            Iterable<? extends VertexiumObject> edges = getGraph().getEdges(ids.getEdgeIds(), fetchHints, filterParameters.getAuthorizations());
            items.add(edges);
        }
        if (ids.getExtendedDataIds().size() > 0) {
            Iterable<? extends VertexiumObject> extendedDataRows = getGraph().getExtendedData(ids.getExtendedDataIds(), filterParameters.getAuthorizations());
            items.add(extendedDataRows);
        }
        Iterable<VertexiumObject> vertexiumObjects = new JoinIterable<>(items);
        vertexiumObjects = sortVertexiumObjectsByResultOrder(vertexiumObjects, ids.getIds());

        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return createIterable(response, filterParameters, vertexiumObjects, evaluateQueryString, false, evaluateSortContainers, response.getTookInMillis(), hits);
    }

    public QueryResultsIterable<SearchHit> search(EnumSet<VertexiumObjectType> objectTypes) {
        if (shouldUseScrollApi()) {
            return searchScroll(objectTypes);
        }
        return searchPaged(objectTypes);
    }

    private QueryInfiniteScrollIterable<SearchHit> searchScroll(EnumSet<VertexiumObjectType> objectTypes) {
        return new QueryInfiniteScrollIterable<SearchHit>(objectTypes) {
            @Override
            protected ElasticsearchGraphQueryIterable<SearchHit> searchResponseToIterable(SearchResponse searchResponse) {
                return ElasticsearchSearchQueryBase.this.searchResponseToSearchHitsIterable(searchResponse);
            }
        };
    }

    private PagingIterable<SearchHit> searchPaged(EnumSet<VertexiumObjectType> objectTypes) {
        return new PagingIterable<SearchHit>(getParameters().getSkip(), getParameters().getLimit(), pageSize) {
            @Override
            protected ElasticsearchGraphQueryIterable<SearchHit> getPageIterable(int skip, int limit, boolean includeAggregations) {
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticsearchDocumentType.fromVertexiumObjectTypes(objectTypes), skip, limit, includeAggregations);
                } catch (IndexNotFoundException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (VertexiumNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }

                return searchResponseToSearchHitsIterable(response);
            }
        };
    }

    private ElasticsearchGraphQueryIterable<SearchHit> searchResponseToSearchHitsIterable(SearchResponse response) {
        SearchHits hits = response.getHits();
        QueryParameters filterParameters = getParameters().clone();
        Iterable<SearchHit> hitsIterable = IterableUtils.toIterable(hits.hits());
        return createIterable(response, filterParameters, hitsIterable, false, false, false, response.getTookInMillis(), hits);
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return new ElasticsearchGraphQueryIdIterable<>(search(EnumSet.of(VertexiumObjectType.VERTEX)));
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return new ElasticsearchGraphQueryIdIterable<>(search(EnumSet.of(VertexiumObjectType.EDGE)));
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds() {
        return new ElasticsearchGraphQueryIdIterable<>(search(EnumSet.of(VertexiumObjectType.EXTENDED_DATA)));
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return new ElasticsearchGraphQueryIdIterable<>(search(VertexiumObjectType.ELEMENTS));
    }

    private <T extends VertexiumObject> Iterable<T> sortVertexiumObjectsByResultOrder(Iterable<T> vertexiumObjects, List<String> ids) {
        ImmutableMap<String, T> itemMap = Maps.uniqueIndex(vertexiumObjects, vertexiumObject -> {
            if (vertexiumObject instanceof Element) {
                return ((Element) vertexiumObject).getId();
            } else if (vertexiumObject instanceof ExtendedDataRow) {
                return ElasticsearchExtendedDataIdUtils.toDocId(((ExtendedDataRow) vertexiumObject).getId());
            } else {
                throw new VertexiumException("Unhandled searchable item type: " + vertexiumObject.getClass().getName());
            }
        });

        List<T> results = new ArrayList<>();
        for (String id : ids) {
            T item = itemMap.get(id);
            if (item != null) {
                results.add(item);
            }
        }
        return results;
    }

    private <T> EmptyElasticsearchGraphQueryIterable<T> createEmptyIterable() {
        return new EmptyElasticsearchGraphQueryIterable<>(ElasticsearchSearchQueryBase.this, getParameters());
    }

    protected <T> ElasticsearchGraphQueryIterable<T> createIterable(
            SearchResponse response,
            QueryParameters filterParameters,
            Iterable<T> vertexiumObjects,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            long searchTimeInMillis,
            SearchHits hits
    ) {
        return new ElasticsearchGraphQueryIterable<>(
                this,
                response,
                filterParameters,
                vertexiumObjects,
                evaluateQueryString,
                evaluateHasContainers,
                evaluateSortContainers,
                hits.getTotalHits(),
                searchTimeInMillis * 1000000,
                hits
        );
    }

    private SearchResponse getSearchResponse(EnumSet<ElasticsearchDocumentType> elementType, int skip, int limit, boolean includeAggregations) {
        SearchRequestBuilder q = buildQuery(elementType, includeAggregations)
                .setFrom(skip)
                .setSize(limit);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("query: %s", q);
        }

        SearchResponse searchResponse = q.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "elasticsearch results %d of %d (time: %dms)",
                    hits.hits().length,
                    hits.getTotalHits(),
                    searchResponse.getTookInMillis()
            );
        }
        return searchResponse;
    }

    protected QueryBuilder getFilterForHasNotPropertyContainer(HasNotPropertyContainer hasNotProperty) {
        String[] propertyNames;
        try {
            propertyNames = getPropertyNames(hasNotProperty.getKey());
            if (propertyNames.length == 0) {
                throw new VertexiumNoMatchingPropertiesException(hasNotProperty.getKey());
            }
        } catch (VertexiumNoMatchingPropertiesException ex) {
            // If we can't find a property this means it doesn't exist on any elements so the hasNot query should
            // match all records.
            return QueryBuilders.matchAllQuery();
        }
        PropertyDefinition propDef = getPropertyDefinition(hasNotProperty.getKey());
        List<QueryBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(propertyName)));
            if (GeoShape.class.isAssignableFrom(propDef.getDataType())) {
                filters.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.GEO_PROPERTY_NAME_SUFFIX)));
            } else if (isExactMatchPropertyDefinition(propDef)) {
                filters.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX)));
            }
        }
        return getSingleFilterOrAndTheFilters(filters, hasNotProperty);
    }

    private QueryBuilder getFilterForHasExtendedData(HasExtendedData has) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (HasExtendedDataFilter hasExtendedDataFilter : has.getFilters()) {
            boolQuery.should(getFilterForHasExtendedDataFilter(hasExtendedDataFilter));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    private QueryBuilder getFilterForHasExtendedDataFilter(HasExtendedDataFilter has) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean hasQuery = false;
        if (has.getElementType() != null) {
            boolQuery.must(
                    QueryBuilders.termQuery(
                            Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                            ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(has.getElementType()).getKey()
                    )
            );
            hasQuery = true;
        }
        if (has.getElementId() != null) {
            boolQuery.must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME, has.getElementId()));
            hasQuery = true;
        }
        if (has.getTableName() != null) {
            boolQuery.must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME, has.getTableName()));
            hasQuery = true;
        }
        if (!hasQuery) {
            throw new VertexiumException("Cannot include a hasExtendedData clause with all nulls");
        }
        return boolQuery;
    }

    protected QueryBuilder getFilterForHasPropertyContainer(HasPropertyContainer hasProperty) {
        String[] propertyNames = getPropertyNames(hasProperty.getKey());
        if (propertyNames.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(hasProperty.getKey());
        }
        PropertyDefinition propDef = getPropertyDefinition(hasProperty.getKey());
        if (propDef == null) {
            throw new VertexiumException("Could not find property definition for property name: " + hasProperty.getKey());
        }
        List<QueryBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(QueryBuilders.existsQuery(propertyName));
            if (GeoShape.class.isAssignableFrom(propDef.getDataType())) {
                filters.add(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.GEO_PROPERTY_NAME_SUFFIX));
            } else if (isExactMatchPropertyDefinition(propDef)) {
                filters.add(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX));
            }
        }
        return getSingleFilterOrOrTheFilters(filters, hasProperty);
    }

    protected QueryBuilder getFiltersForHasValueContainer(HasValueContainer has) {
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

    protected QueryBuilder getFilterForGeoComparePredicate(GeoCompare compare, HasValueContainer has) {
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }

        if (!(has.value instanceof GeoShape)) {
            throw new VertexiumNotSupportedException("GeoCompare searches only accept values of type GeoShape");
        }

        List<QueryBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            String propertyName = key + Elasticsearch5SearchIndex.GEO_PROPERTY_NAME_SUFFIX;

            String inflatedPropertyName = getSearchIndex().removeVisibilityFromPropertyName(propertyName);
            PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(inflatedPropertyName);

            if (propertyDefinition != null && !GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
                throw new VertexiumNotSupportedException("Unable to perform geo query on field of type: " + propertyDefinition.getDataType().getName());
            }

            GeoShape value = (GeoShape)has.value;
            if (value instanceof GeoHash) {
                value = ((GeoHash) value).toGeoRect();
            }

            ShapeBuilder shapeBuilder = getShapeBuilder(value);
            ShapeRelation relation = ShapeRelation.getRelationByName(compare.getCompareName());
            filters.add(new GeoShapeQueryBuilder(propertyName, shapeBuilder).relation(relation));
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    private ShapeBuilder getShapeBuilder(GeoShape geoShape) {
        if (geoShape instanceof GeoCircle) {
            GeoCircle geoCircle = (GeoCircle) geoShape;
            return ShapeBuilders.newCircleBuilder()
                    .center(geoCircle.getLongitude(), geoCircle.getLatitude())
                    .radius(geoCircle.getRadius(), DistanceUnit.KILOMETERS);
        } else if (geoShape instanceof GeoRect) {
            GeoRect geoRect = (GeoRect) geoShape;
            Coordinate topLeft = new Coordinate(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude());
            Coordinate bottomRight = new Coordinate(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude());
            return ShapeBuilders.newEnvelope(topLeft, bottomRight);
        } else if (geoShape instanceof GeoCollection) {
            GeometryCollectionBuilder shapeBuilder = ShapeBuilders.newGeometryCollection();
            ((GeoCollection) geoShape).getGeoShapes().forEach(shape -> shapeBuilder.shape(getShapeBuilder(shape)));
            return shapeBuilder;
        } else if (geoShape instanceof GeoLine) {
            List<Coordinate> coordinates = ((GeoLine) geoShape).getGeoPoints().stream()
                    .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                    .collect(Collectors.toList());
            return ShapeBuilders.newLineString(coordinates);
        } else if (geoShape instanceof GeoPoint) {
            GeoPoint geoPoint = (GeoPoint) geoShape;
            return ShapeBuilders.newPoint(geoPoint.getLongitude(), geoPoint.getLatitude());
        } else if (geoShape instanceof GeoPolygon) {
            GeoPolygon geoPolygon = (GeoPolygon) geoShape;
            List<Coordinate> shell = geoPolygon.getOuterBoundary().stream()
                    .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                    .collect(Collectors.toList());
            PolygonBuilder polygonBuilder = ShapeBuilders.newPolygon(shell);
            geoPolygon.getHoles().forEach(hole -> {
                List<Coordinate> coordinates = hole.stream()
                        .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                        .collect(Collectors.toList());
                polygonBuilder.hole(ShapeBuilders.newLineString(coordinates));
            });
            return polygonBuilder;
        } else {
            throw new VertexiumException("Unexpected has value type " + geoShape.getClass().getName());
        }
    }

    private QueryBuilder getSingleFilterOrOrTheFilters(List<QueryBuilder> filters, HasContainer has) {
        if (filters.size() > 1) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.should(filter);
            }
            boolQuery.minimumShouldMatch(1);
            return boolQuery;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter for: " + has);
        }
    }

    private QueryBuilder getSingleFilterOrAndTheFilters(List<QueryBuilder> filters, HasContainer has) {
        if (filters.size() > 1) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.must(filter);
            }
            return boolQuery;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new VertexiumException("Unexpected filter count, expected at least 1 filter for: " + has);
        }
    }

    protected QueryBuilder getFilterForTextPredicate(TextPredicate compare, HasValueContainer has) {
        Object value = has.value;
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<QueryBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (value instanceof String) {
                value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
            }
            switch (compare) {
                case CONTAINS:
                    if (value instanceof String) {
                        String[] terms = splitStringIntoTerms((String) value);
                        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                        for (String term : terms) {
                            boolQueryBuilder.must(QueryBuilders.termQuery(key, term));
                        }
                        filters.add(boolQueryBuilder);
                    } else {
                        filters.add(QueryBuilders.termQuery(key, value));
                    }
                    break;
                case DOES_NOT_CONTAIN:
                    BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                    if (value instanceof String) {
                        String[] terms = splitStringIntoTerms((String) value);
                        filters.add(boolQueryBuilder.mustNot(QueryBuilders.termsQuery(key, terms)));
                    } else {
                        filters.add(boolQueryBuilder.mustNot(QueryBuilders.termQuery(key, value)));
                    }
                    break;
                default:
                    throw new VertexiumException("Unexpected text predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    protected QueryBuilder getFilterForContainsPredicate(Contains contains, HasValueContainer has) {
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            if (contains.equals(Contains.NOT_IN)) {
                return QueryBuilders.matchAllQuery();
            }
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<QueryBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (has.value instanceof Iterable) {
                has.value = IterableUtils.toArray((Iterable<?>) has.value, Object.class);
            }
            if (has.value instanceof String
                    || has.value instanceof String[]
                    || (has.value instanceof Object[] && ((Object[]) has.value).length > 0 && ((Object[]) has.value)[0] instanceof String)
                    ) {
                key = key + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }
            switch (contains) {
                case IN:
                    filters.add(QueryBuilders.termsQuery(key, (Object[]) has.value));
                    break;
                case NOT_IN:
                    filters.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(key, (Object[]) has.value)));
                    break;
                default:
                    throw new VertexiumException("Unexpected Contains predicate " + has.predicate);
            }
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    protected QueryBuilder getFilterForComparePredicate(Compare compare, HasValueContainer has) {
        Object value = convertQueryValue(has.value);
        String[] keys = getPropertyNames(has.key);
        if (keys.length == 0) {
            if (compare.equals(Compare.NOT_EQUAL)) {
                return QueryBuilders.matchAllQuery();
            }
            throw new VertexiumNoMatchingPropertiesException(has.key);
        }
        List<QueryBuilder> filters = new ArrayList<>();
        for (String key : keys) {
            if (has.value instanceof IpV4Address) {
                // this value is converted to a string and should not use the exact match field
            } else if (value instanceof String || value instanceof String[]) {
                key = key + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }
            switch (compare) {
                case EQUAL:
                    if (value instanceof DateOnly) {
                        DateOnly dateOnlyValue = ((DateOnly) value);
                        String lower = dateOnlyValue.toString() + "T00:00:00.000Z";
                        String upper = dateOnlyValue.toString() + "T23:59:59.999Z";
                        filters.add(QueryBuilders.rangeQuery(key).gte(lower).lte(upper));
                    } else {
                        filters.add(QueryBuilders.termQuery(key, value));
                    }
                    break;
                case GREATER_THAN_EQUAL:
                    filters.add(QueryBuilders.rangeQuery(key).gte(value));
                    break;
                case GREATER_THAN:
                    filters.add(QueryBuilders.rangeQuery(key).gt(value));
                    break;
                case LESS_THAN_EQUAL:
                    filters.add(QueryBuilders.rangeQuery(key).lte(value));
                    break;
                case LESS_THAN:
                    filters.add(QueryBuilders.rangeQuery(key).lt(value));
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

    private Object convertQueryValue(Object value) {
        if (value instanceof Date) {
            return new DateTime(((Date) value).getTime());
        }
        if (value instanceof BigInteger) {
            return ((BigInteger) value).intValue();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        }
        if (value instanceof IpV4Address) {
            return value.toString();
        }
        return value;
    }

    protected String[] getPropertyNames(String propertyName) {
        String[] allMatchingPropertyNames = getSearchIndex().getAllMatchingPropertyNames(getGraph(), propertyName, getParameters().getAuthorizations());
        return Arrays.stream(allMatchingPropertyNames)
                .map(fieldName -> fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT))
                .collect(Collectors.toList())
                .toArray(new String[allMatchingPropertyNames.length]);
    }

    protected Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
    }

    protected void addElementTypeFilter(List<QueryBuilder> filters, EnumSet<ElasticsearchDocumentType> elementType) {
        if (elementType != null) {
            filters.add(createElementTypeFilter(elementType));
        }
    }

    protected TermsQueryBuilder createElementTypeFilter(EnumSet<ElasticsearchDocumentType> elementType) {
        List<String> values = new ArrayList<>();
        for (ElasticsearchDocumentType et : elementType) {
            values.add(et.getKey());
        }
        return QueryBuilders.termsQuery(
                Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                values.toArray(new String[values.size()])
        );
    }

    protected void addNotFilter(List<QueryBuilder> filters, String key, Object value) {
        filters.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(key, value)));
    }

    protected QueryBuilder getFilterBuilder(List<QueryBuilder> filters) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (QueryBuilder filter : filters) {
            boolQuery.must(filter);
        }
        return boolQuery;
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

    protected QueryBuilder createSimilarToTextQuery(SimilarToTextQueryParameters similarTo) {
        List<String> allFields = new ArrayList<>();
        String[] fields = similarTo.getFields();
        for (String field : fields) {
            Collections.addAll(allFields, getPropertyNames(field));
        }
        MoreLikeThisQueryBuilder q = QueryBuilders.moreLikeThisQuery(
                allFields.toArray(new String[allFields.size()]),
                new String[]{similarTo.getText()},
                null
        );
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

    protected List<AggregationBuilder> getElasticsearchAggregations(Iterable<Aggregation> aggregations) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (Aggregation agg : aggregations) {
            if (agg instanceof HistogramAggregation) {
                aggs.addAll(getElasticsearchHistogramAggregations((HistogramAggregation) agg));
            } else if (agg instanceof RangeAggregation) {
                aggs.addAll(getElasticsearchRangeAggregations((RangeAggregation) agg));
            } else if (agg instanceof PercentilesAggregation) {
                aggs.addAll(getElasticsearchPercentilesAggregations((PercentilesAggregation) agg));
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
        PropertyDefinition propertyDefinition = getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new VertexiumException("Unknown property " + agg.getFieldName() + " for geohash aggregation.");
        }
        if (propertyDefinition.getDataType() != GeoPoint.class) {
            throw new VertexiumNotSupportedException("Only GeoPoint properties are valid for Geohash aggregation. Invalid property " + agg.getFieldName());
        }
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            GeoGridAggregationBuilder geoHashAgg = AggregationBuilders.geohashGrid(aggName);
            geoHashAgg.field(propertyName + Elasticsearch5SearchIndex.GEO_POINT_PROPERTY_NAME_SUFFIX);
            geoHashAgg.precision(agg.getPrecision());
            aggs.add(geoHashAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchStatisticsAggregations(StatisticsAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            ExtendedStatsAggregationBuilder statsAgg = AggregationBuilders.extendedStats(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchPercentilesAggregations(PercentilesAggregation agg) {
        String propertyName = getSearchIndex().addVisibilityToPropertyName(getGraph(), agg.getFieldName(), agg.getVisibility());
        String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
        String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
        PercentilesAggregationBuilder percentilesAgg = AggregationBuilders.percentiles(aggName);
        percentilesAgg.field(propertyName);
        if (agg.getPercents() != null && agg.getPercents().length > 0) {
            percentilesAgg.percentiles(agg.getPercents());
        }
        return Collections.singletonList(percentilesAgg);
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
            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), "0"));
            termsAgg.field(fieldName);
            if (agg.getSize() != null) {
                termsAgg.size(agg.getSize());
            }
            termsAgg.shardSize(termAggregationShardSize);
            termsAggs.add(termsAgg);
        } else {
            PropertyDefinition propertyDefinition = getPropertyDefinition(fieldName);
            for (String propertyName : getPropertyNames(fieldName)) {
                if (isExactMatchPropertyDefinition(propertyDefinition)) {
                    propertyName = propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                }

                String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
                TermsAggregationBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), visibilityHash));
                termsAgg.field(propertyName);
                if (agg.getSize() != null) {
                    termsAgg.size(agg.getSize());
                }
                termsAgg.shardSize(termAggregationShardSize);

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
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

    private Collection<? extends AggregationBuilder> getElasticsearchCalendarFieldAggregation(CalendarFieldAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getPropertyDefinition(agg.getPropertyName());
        if (propertyDefinition == null) {
            throw new VertexiumException("Could not find mapping for property: " + agg.getPropertyName());
        }
        Class propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getPropertyNames(agg.getPropertyName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (propertyDataType == Date.class) {
                HistogramAggregationBuilder histAgg = AggregationBuilders.histogram(aggName);
                histAgg.interval(1);
                if (agg.getMinDocumentCount() != null) {
                    histAgg.minDocCount(agg.getMinDocumentCount());
                } else {
                    histAgg.minDocCount(1L);
                }
                Script script = new Script(
                        ScriptType.INLINE,
                        "groovy",
                        getCalendarFieldAggregationScript(agg, propertyName),
                        new HashMap<>()
                );
                histAgg.script(script);

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
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
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (propertyDataType == Date.class) {
                DateHistogramAggregationBuilder dateAgg = AggregationBuilders.dateHistogram(aggName);
                dateAgg.field(propertyName);
                String interval = agg.getInterval();
                if (Pattern.matches("^[0-9\\.]+$", interval)) {
                    interval += "ms";
                }
                dateAgg.dateHistogramInterval(new DateHistogramInterval(interval));
                dateAgg.minDocCount(1L);
                if (agg.getMinDocumentCount() != null) {
                    dateAgg.minDocCount(agg.getMinDocumentCount());
                }
                if (agg.getExtendedBounds() != null) {
                    HistogramAggregation.ExtendedBounds<?> bounds = agg.getExtendedBounds();
                    if (bounds.getMinMaxType().isAssignableFrom(Long.class)) {
                        dateAgg.extendedBounds(new ExtendedBounds((Long) bounds.getMin(), (Long) bounds.getMax()));
                    } else if (bounds.getMinMaxType().isAssignableFrom(Date.class)) {
                        dateAgg.extendedBounds(new ExtendedBounds(
                                new DateTime(bounds.getMin()).toString(),
                                new DateTime(bounds.getMax()).toString()
                        ));
                    } else if (bounds.getMinMaxType().isAssignableFrom(String.class)) {
                        dateAgg.extendedBounds(new ExtendedBounds((String) bounds.getMin(), (String) bounds.getMax()));
                    } else {
                        throw new VertexiumException("Unhandled extended bounds type. Expected Long, String, or Date. Found: " + bounds.getMinMaxType().getName());
                    }
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    dateAgg.subAggregation(subAgg);
                }

                aggs.add(dateAgg);
            } else {
                HistogramAggregationBuilder histogramAgg = AggregationBuilders.histogram(aggName);
                histogramAgg.field(propertyName);
                histogramAgg.interval(Long.parseLong(agg.getInterval()));
                histogramAgg.minDocCount(1L);
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

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    histogramAgg.subAggregation(subAgg);
                }

                aggs.add(histogramAgg);
            }
        }
        return aggs;
    }

    protected List<AggregationBuilder> getElasticsearchRangeAggregations(RangeAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new VertexiumException("Could not find mapping for property: " + agg.getFieldName());
        }
        Class propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getPropertyNames(agg.getFieldName())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (propertyDataType == Date.class) {
                DateRangeAggregationBuilder dateRangeBuilder = AggregationBuilders.dateRange(aggName);
                dateRangeBuilder.field(propertyName);

                if (!Strings.isNullOrEmpty(agg.getFormat())) {
                    dateRangeBuilder.format(agg.getFormat());
                }

                for (RangeAggregation.Range range : agg.getRanges()) {
                    applyRange(dateRangeBuilder, range);
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    dateRangeBuilder.subAggregation(subAgg);
                }

                aggs.add(dateRangeBuilder);
            } else {
                RangeAggregationBuilder rangeBuilder = AggregationBuilders.range(aggName);
                rangeBuilder.field(propertyName);

                if (!Strings.isNullOrEmpty(agg.getFormat())) {
                    throw new VertexiumException("Invalid use of format for property: " + agg.getFieldName() +
                                                         ". Format is only valid for date properties");
                }

                for (RangeAggregation.Range range : agg.getRanges()) {
                    Object from = range.getFrom();
                    Object to = range.getTo();
                    if ((from != null && !(from instanceof Number)) || (to != null && !(to instanceof Number))) {
                        throw new VertexiumException("Invalid range for property: " + agg.getFieldName() +
                                                             ". Both to and from must be Numeric.");
                    }
                    rangeBuilder.addRange(
                            range.getKey(),
                            from == null ? Double.MIN_VALUE : ((Number) from).doubleValue(),
                            to == null ? Double.MAX_VALUE : ((Number) to).doubleValue()
                    );
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    rangeBuilder.subAggregation(subAgg);
                }

                aggs.add(rangeBuilder);
            }
        }
        return aggs;
    }

    private void applyRange(DateRangeAggregationBuilder dateRangeBuilder, RangeAggregation.Range range) {
        Object from = range.getFrom();
        Object to = range.getTo();
        if ((from == null || from instanceof String) && (to == null || to instanceof String)) {
            String fromString = (String) from;
            String toString = (String) to;
            dateRangeBuilder.addRange(range.getKey(), fromString, toString);
        } else if ((from == null || from instanceof Number) && (to == null || to instanceof Number)) {
            double fromDouble = from == null ? null : ((Number) from).doubleValue();
            double toDouble = to == null ? null : ((Number) to).doubleValue();
            dateRangeBuilder.addRange(range.getKey(), fromDouble, toDouble);
        } else if ((from == null || from instanceof DateTime) && (to == null || to instanceof DateTime)) {
            DateTime fromDateTime = (DateTime) from;
            DateTime toDateTime = (DateTime) to;
            dateRangeBuilder.addRange(range.getKey(), fromDateTime, toDateTime);
        } else if ((from == null || from instanceof Date) && (to == null || to instanceof Date)) {
            DateTime fromDateTime = from == null ? null : new DateTime(((Date) from).getTime());
            DateTime toDateTime = to == null ? null : new DateTime(((Date) to).getTime());
            dateRangeBuilder.addRange(range.getKey(), fromDateTime, toDateTime);
        } else {
            String fromClassName = from == null ? null : from.getClass().getName();
            String toClassName = to == null ? null : to.getClass().getName();
            throw new VertexiumException("unhandled range types " + fromClassName + ", " + toClassName);
        }
    }

    protected PropertyDefinition getPropertyDefinition(String propertyName) {
        return getGraph().getPropertyDefinition(propertyName);
    }

    private boolean shouldUseScrollApi() {
        return getParameters().getSkip() == 0 && (getParameters().getLimit() == null || getParameters().getLimit() > pagingLimit);
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

    private abstract class QueryInfiniteScrollIterable<T> extends InfiniteScrollIterable<T> {
        private final EnumSet<VertexiumObjectType> objectTypes;

        public QueryInfiniteScrollIterable(EnumSet<VertexiumObjectType> objectTypes) {
            this.objectTypes = objectTypes;
        }

        @Override
        protected SearchResponse getInitialSearchResponse() {
            try {
                SearchRequestBuilder q = buildQuery(ElasticsearchDocumentType.fromVertexiumObjectTypes(objectTypes), true)
                        .setScroll(scrollKeepAlive);
                if (QUERY_LOGGER.isTraceEnabled()) {
                    QUERY_LOGGER.trace("query: %s", q);
                }
                return q.execute().actionGet();
            } catch (IndexNotFoundException ex) {
                LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                return null;
            } catch (VertexiumNoMatchingPropertiesException ex) {
                LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                return null;
            }
        }

        @Override
        protected SearchResponse getNextSearchResponse(String scrollId) {
            try {
                return client.prepareSearchScroll(scrollId)
                        .setScroll(scrollKeepAlive)
                        .execute().actionGet();
            } catch (Exception ex) {
                throw new VertexiumException("Failed to request more items from scroll " + scrollId, ex);
            }
        }

        @Override
        protected void closeScroll(String scrollId) {
            ElasticsearchSearchQueryBase.this.closeScroll(scrollId);
        }
    }

    private static class Ids {
        private final List<String> vertexIds;
        private final List<String> edgeIds;
        private final List<String> ids;
        private final List<ExtendedDataRowId> extendedDataIds;

        public Ids(SearchHits hits) {
            vertexIds = new ArrayList<>();
            edgeIds = new ArrayList<>();
            extendedDataIds = new ArrayList<>();
            ids = new ArrayList<>();
            for (SearchHit hit : hits) {
                ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
                if (dt == null) {
                    continue;
                }
                String id = hit.getId();
                switch (dt) {
                    case VERTEX:
                        ids.add(id);
                        vertexIds.add(id);
                        break;
                    case EDGE:
                        ids.add(id);
                        edgeIds.add(id);
                        break;
                    case VERTEX_EXTENDED_DATA:
                    case EDGE_EXTENDED_DATA:
                        ids.add(id);
                        extendedDataIds.add(ElasticsearchExtendedDataIdUtils.fromSearchHit(hit));
                        break;
                    default:
                        LOGGER.warn("Unhandled document type: %s", dt);
                        break;
                }
            }
        }

        public List<String> getVertexIds() {
            return vertexIds;
        }

        public List<String> getEdgeIds() {
            return edgeIds;
        }

        public List<String> getIds() {
            return ids;
        }

        public List<ExtendedDataRowId> getExtendedDataIds() {
            return extendedDataIds;
        }
    }

    @SuppressWarnings("unused")
    public static class Options {
        public int pageSize;
        public ScoringStrategy scoringStrategy;
        public IndexSelectionStrategy indexSelectionStrategy;
        public TimeValue scrollKeepAlive;
        public StandardAnalyzer analyzer = new StandardAnalyzer();
        public int pagingLimit;
        public int termAggregationShardSize;

        public int getPageSize() {
            return pageSize;
        }

        public Options setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public ScoringStrategy getScoringStrategy() {
            return scoringStrategy;
        }

        public Options setScoringStrategy(ScoringStrategy scoringStrategy) {
            this.scoringStrategy = scoringStrategy;
            return this;
        }

        public IndexSelectionStrategy getIndexSelectionStrategy() {
            return indexSelectionStrategy;
        }

        public Options setIndexSelectionStrategy(IndexSelectionStrategy indexSelectionStrategy) {
            this.indexSelectionStrategy = indexSelectionStrategy;
            return this;
        }

        public TimeValue getScrollKeepAlive() {
            return scrollKeepAlive;
        }

        public Options setScrollKeepAlive(TimeValue scrollKeepAlive) {
            this.scrollKeepAlive = scrollKeepAlive;
            return this;
        }

        public StandardAnalyzer getAnalyzer() {
            return analyzer;
        }

        public Options setAnalyzer(StandardAnalyzer analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public int getPagingLimit() {
            return pagingLimit;
        }

        public Options setPagingLimit(int pagingLimit) {
            this.pagingLimit = pagingLimit;
            return this;
        }

        public int getTermAggregationShardSize () { return termAggregationShardSize; }

        public Options setTermAggregationShardSize (int termAggregationShardSize) {
            this.termAggregationShardSize = termAggregationShardSize;
            return this;
        }
    }
}

