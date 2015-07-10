package org.vertexium.elasticsearch;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.query.*;
import org.vertexium.type.GeoCircle;
import org.vertexium.util.*;

import java.io.IOException;
import java.util.*;

public abstract class ElasticSearchQueryBase extends QueryBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchQueryBase.class);
    public static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Query.class);
    private final TransportClient client;
    private final boolean evaluateHasContainers;
    private final StandardAnalyzer analyzer;
    private String[] indicesToQuery;
    private ScoringStrategy scoringStrategy;
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    protected ElasticSearchQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            boolean evaluateHasContainers,
            Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.client = client;
        this.indicesToQuery = indicesToQuery;
        this.evaluateHasContainers = evaluateHasContainers;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
    }

    protected ElasticSearchQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String[] similarToFields, String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            boolean evaluateHasContainers,
            Authorizations authorizations) {
        super(graph, similarToFields, similarToText, propertyDefinitions, authorizations);
        this.client = client;
        this.indicesToQuery = indicesToQuery;
        this.evaluateHasContainers = evaluateHasContainers;
        this.scoringStrategy = scoringStrategy;
        this.analyzer = new StandardAnalyzer();
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
    }

    @Override
    public Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints) {
        long startTime = System.nanoTime();
        SearchResponse response = getSearchResponse(ElasticSearchSearchIndexBase.ELEMENT_TYPE_VERTEX);
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
        return createIterable(response, filterParameters, vertices, evaluateHasContainers, searchTime, hits);
    }

    @Override
    public Iterable<Edge> edges(EnumSet<FetchHint> fetchHints) {
        long startTime = System.nanoTime();
        SearchResponse response = getSearchResponse(ElasticSearchSearchIndexBase.ELEMENT_TYPE_EDGE);
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
        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return createIterable(response, filterParameters, edges, evaluateHasContainers, searchTime, hits);
    }

    @Override
    public Iterable<Element> elements(EnumSet<FetchHint> fetchHints) {
        long startTime = System.nanoTime();
        SearchResponse response = getSearchResponse(null);
        final SearchHits hits = response.getHits();
        List<String> vertexIds = new ArrayList<>();
        List<String> edgeIds = new ArrayList<>();
        for (SearchHit hit : hits) {
            SearchHitField elementType = hit.getFields().get(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME);
            if (elementType == null) {
                continue;
            }
            String elementTypeString = elementType.getValue().toString();
            switch (elementTypeString) {
                case ElasticSearchSearchIndexBase.ELEMENT_TYPE_VERTEX:
                    vertexIds.add(hit.getId());
                    break;
                case ElasticSearchSearchIndexBase.ELEMENT_TYPE_EDGE:
                    edgeIds.add(hit.getId());
                    break;
                default:
                    LOGGER.warn("Unhandled element type returned: %s", elementTypeString);
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
        Iterable<Vertex> vertices = getGraph().getVertices(vertexIds, fetchHints, filterParameters.getAuthorizations());
        Iterable<Edge> edges = getGraph().getEdges(edgeIds, fetchHints, filterParameters.getAuthorizations());
        Iterable<Element> elements = new JoinIterable<>(new ToElementIterable<>(vertices), new ToElementIterable<>(edges));
        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return createIterable(response, filterParameters, elements, evaluateHasContainers, searchTime, hits);
    }

    protected <T extends Element> ElasticSearchGraphQueryIterable<T> createIterable(SearchResponse response, QueryParameters filterParameters, Iterable<T> elements, boolean evaluateHasContainers, long searchTime, SearchHits hits) {
        return new ElasticSearchGraphQueryIterable<>(response, filterParameters, elements, false, evaluateHasContainers, hits.getTotalHits(), searchTime, hits);
    }

    private SearchResponse getSearchResponse(String elementType) {
        List<FilterBuilder> filters = getFilters(elementType);
        QueryBuilder query = createQuery(getParameters(), elementType, filters);
        query = scoringStrategy.updateQuery(query);
        SearchRequestBuilder q = getSearchRequestBuilder(filters, query);

        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("query: %s", q);
        }
        return q.execute()
                .actionGet();
    }

    protected List<FilterBuilder> getFilters(String elementType) {
        List<FilterBuilder> filters = new ArrayList<>();
        if (elementType != null) {
            addElementTypeFilter(filters, elementType);
        }
        for (HasContainer has : getParameters().getHasContainers()) {
            if (has instanceof HasValueContainer) {
                getFiltersForHasValueContainer(filters, (HasValueContainer) has);
            } else if (has instanceof HasPropertyContainer) {
                getFiltersForHasPropertyContainer(filters, (HasPropertyContainer) has);
            } else if (has instanceof HasNotPropertyContainer) {
                getFiltersForHasNotPropertyContainer(filters, (HasNotPropertyContainer) has);
            } else {
                throw new VertexiumException("Unexpected type " + has.getClass().getName());
            }
        }
        return filters;
    }

    protected void getFiltersForHasNotPropertyContainer(List<FilterBuilder> filters, HasNotPropertyContainer hasNotProperty) {
        filters.add(FilterBuilders.notFilter(FilterBuilders.existsFilter(nameSubstitutionStrategy.deflate(hasNotProperty.getKey()))));
    }

    protected void getFiltersForHasPropertyContainer(List<FilterBuilder> filters, HasPropertyContainer hasProperty) {
        filters.add(FilterBuilders.existsFilter(nameSubstitutionStrategy.deflate(hasProperty.getKey())));
    }

    protected void getFiltersForHasValueContainer(List<FilterBuilder> filters, HasValueContainer has) {
        if (has.predicate instanceof Compare) {
            getFiltersForComparePredicate(filters, (Compare) has.predicate, has);
        } else if (has.predicate instanceof Contains) {
            getFiltersForContainsPredicate(filters, (Contains) has.predicate, has);
        } else if (has.predicate instanceof TextPredicate) {
            getFiltersForTextPredicate(filters, (TextPredicate) has.predicate, has);
        } else if (has.predicate instanceof GeoCompare) {
            getFiltersForGeoComparePredicate(filters, (GeoCompare) has.predicate, has);
        } else {
            throw new VertexiumException("Unexpected predicate type " + has.predicate.getClass().getName());
        }
    }

    protected void getFiltersForGeoComparePredicate(List<FilterBuilder> filters, GeoCompare compare, HasValueContainer has) {
        String propertyName = nameSubstitutionStrategy.deflate(has.key) + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX;
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

    protected void getFiltersForTextPredicate(List<FilterBuilder> filters, TextPredicate compare, HasValueContainer has) {
        Object value = has.value;
        String key = nameSubstitutionStrategy.deflate(has.key);
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

    protected void getFiltersForContainsPredicate(List<FilterBuilder> filters, Contains contains, HasValueContainer has) {
        Object value = has.value;
        String key = nameSubstitutionStrategy.deflate(has.key);
        if (value instanceof String || value instanceof String[]) {
            key = key + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
        }
        if (has.value instanceof Iterable) {
            has.value = IterableUtils.toArray((Iterable<?>) has.value, Object.class);
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

    protected void getFiltersForComparePredicate(List<FilterBuilder> filters, Compare compare, HasValueContainer has) {
        Object value = has.value;
        String key = nameSubstitutionStrategy.deflate(has.key);
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

    protected void addElementTypeFilter(List<FilterBuilder> filters, String elementType) {
        if (elementType != null) {
            filters.add(createElementTypeFilter(elementType));
        }
    }

    protected TermsFilterBuilder createElementTypeFilter(String elementType) {
        return FilterBuilders.inFilter(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME, elementType);
    }

    protected void addNotFilter(List<FilterBuilder> filters, String key, Object value) {
        filters.add(FilterBuilders.notFilter(FilterBuilders.inFilter(key, value)));
    }

    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder) {
        AndFilterBuilder filterBuilder = getFilterBuilder(filters);
        return getClient()
                .prepareSearch(getIndicesToQuery())
                .setTypes(ElasticSearchSearchIndexBase.ELEMENT_TYPE)
                .setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder))
                .addField(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME)
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());
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

    protected QueryBuilder createQuery(QueryParameters queryParameters, String elementType, List<FilterBuilder> filters) {
        if (queryParameters instanceof QueryStringQueryParameters) {
            String queryString = ((QueryStringQueryParameters) queryParameters).getQueryString();
            if (queryString == null || queryString.equals("*")) {
                return QueryBuilders.matchAllQuery();
            }
            return QueryBuilders.queryString(queryString);
        } else if (queryParameters instanceof SimilarToTextQueryParameters) {
            SimilarToTextQueryParameters similarTo = (SimilarToTextQueryParameters) queryParameters;
            MoreLikeThisQueryBuilder q = QueryBuilders.moreLikeThisQuery(similarTo.getFields())
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
        } else {
            throw new VertexiumException("Query parameters not supported of type: " + queryParameters.getClass().getName());
        }
    }

    public TransportClient getClient() {
        return client;
    }

    public String[] getIndicesToQuery() {
        return indicesToQuery;
    }

    protected static void addGeohashQueryToSearchRequestBuilder(SearchRequestBuilder searchRequestBuilder, List<GeohashQueryItem> geohashQueryItems) {
        for (GeohashQueryItem geohashQueryItem : geohashQueryItems) {
            GeoHashGridBuilder agg = AggregationBuilders.geohashGrid(geohashQueryItem.getAggregationName());
            agg.field(geohashQueryItem.getFieldName());
            agg.precision(geohashQueryItem.getPrecision());
            searchRequestBuilder.addAggregation(agg);
        }
    }

    protected static void addTermsQueryToSearchRequestBuilder(SearchRequestBuilder searchRequestBuilder, List<TermsQueryItem> termsQueryItems, Map<String, PropertyDefinition> propertyDefinitions) {
        for (TermsQueryItem termsQueryItem : termsQueryItems) {
            String fieldName = termsQueryItem.getFieldName();
            PropertyDefinition propertyDefinition = propertyDefinitions.get(fieldName);
            if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                fieldName = propertyDefinition.getPropertyName() + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }

            TermsBuilder agg = AggregationBuilders.terms(termsQueryItem.getAggregationName());
            agg.field(fieldName);
            searchRequestBuilder.addAggregation(agg);
        }
    }

    protected static void addHistogramQueryToSearchRequestBuilder(SearchRequestBuilder searchRequestBuilder, List<HistogramQueryItem> histogramQueryItems, Map<String, PropertyDefinition> propertyDefinitions) {
        for (HistogramQueryItem histogramQueryItem : histogramQueryItems) {
            PropertyDefinition propertyDefinition = propertyDefinitions.get(histogramQueryItem.getFieldName());
            if (propertyDefinition == null) {
                throw new VertexiumException("Could not find mapping for property: " + histogramQueryItem.getFieldName());
            }
            Class propertyDataType = propertyDefinition.getDataType();
            if (propertyDataType == Date.class) {
                DateHistogramBuilder agg = AggregationBuilders.dateHistogram(histogramQueryItem.getAggregationName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            } else {
                HistogramBuilder agg = AggregationBuilders.histogram(histogramQueryItem.getAggregationName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            }
        }
    }
}
