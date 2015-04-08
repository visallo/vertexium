package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.type.GeoCircle;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.IterableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.*;
import org.vertexium.query.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public abstract class ElasticSearchGraphQueryBase extends GraphQueryBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchGraphQueryBase.class);
    private final TransportClient client;
    private final boolean evaluateHasContainers;
    private String[] indicesToQuery;
    private ScoringStrategy scoringStrategy;

    protected ElasticSearchGraphQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            boolean evaluateHasContainers,
            Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.client = client;
        this.indicesToQuery = indicesToQuery;
        this.evaluateHasContainers = evaluateHasContainers;
        this.scoringStrategy = scoringStrategy;
    }

    protected ElasticSearchGraphQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String[] similarToFields, String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            boolean evaluateHasContainers,
            Authorizations authorizations) {
        super(graph, similarToFields, similarToText, propertyDefinitions, authorizations);
        this.client = client;
        this.indicesToQuery = indicesToQuery;
        this.evaluateHasContainers = evaluateHasContainers;
        this.scoringStrategy = scoringStrategy;
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
            LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits() + " (time: " + (searchTime / 1000 / 1000) + "ms)");
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
            LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits() + " (time: " + ((endTime - startTime) / 1000 / 1000) + "ms)");
        }

        // since ES doesn't support security we will rely on the graph to provide edge filtering
        // and rely on the DefaultGraphQueryIterable to provide property filtering
        QueryParameters filterParameters = getParameters().clone();
        filterParameters.setSkip(0); // ES already did a skip
        Iterable<Edge> edges = getGraph().getEdges(ids, fetchHints, filterParameters.getAuthorizations());
        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return createIterable(response, filterParameters, edges, evaluateHasContainers, searchTime, hits);
    }

    protected <T extends Element> ElasticSearchGraphQueryIterable<T> createIterable(SearchResponse response, QueryParameters filterParameters, Iterable<T> elements, boolean evaluateHasContainers, long searchTime, SearchHits hits) {
        return new ElasticSearchGraphQueryIterable<>(response, filterParameters, elements, false, evaluateHasContainers, hits.getTotalHits(), searchTime, hits);
    }

    private SearchResponse getSearchResponse(String elementType) {
        List<FilterBuilder> filters = getFilters(elementType);
        QueryBuilder query = createQuery(getParameters(), elementType, filters);
        query = scoringStrategy.updateQuery(query);
        SearchRequestBuilder q = getSearchRequestBuilder(filters, query);

        LOGGER.debug("query: " + q);
        return q.execute()
                .actionGet();
    }

    protected List<FilterBuilder> getFilters(String elementType) {
        List<FilterBuilder> filters = new ArrayList<>();
        addElementTypeFilter(filters, elementType);
        for (HasContainer has : getParameters().getHasContainers()) {
            if (has.predicate instanceof Compare) {
                Compare compare = (Compare) has.predicate;
                Object value = has.value;
                String key = has.key;
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
                    case IN:
                        filters.add(FilterBuilders.inFilter(key, (Object[]) has.value));
                        break;
                    default:
                        throw new VertexiumException("Unexpected Compare predicate " + has.predicate);
                }
            } else if (has.predicate instanceof TextPredicate) {
                TextPredicate compare = (TextPredicate) has.predicate;
                Object value = has.value;
                if (value instanceof String) {
                    value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
                }
                switch (compare) {
                    case CONTAINS:
                        if (value instanceof String) {
                            filters.add(FilterBuilders.termsFilter(has.key, splitStringIntoTerms((String) value)).execution("and"));
                        } else {
                            filters.add(FilterBuilders.termFilter(has.key, value));
                        }
                        break;
                    default:
                        throw new VertexiumException("Unexpected text predicate " + has.predicate);
                }
            } else if (has.predicate instanceof GeoCompare) {
                GeoCompare compare = (GeoCompare) has.predicate;
                switch (compare) {
                    case WITHIN:
                        if (has.value instanceof GeoCircle) {
                            GeoCircle geoCircle = (GeoCircle) has.value;
                            double lat = geoCircle.getLatitude();
                            double lon = geoCircle.getLongitude();
                            double distance = geoCircle.getRadius();
                            filters
                                    .add(FilterBuilders
                                            .geoDistanceFilter(has.key + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX)
                                            .point(lat, lon)
                                            .distance(distance, DistanceUnit.KILOMETERS));
                        } else {
                            throw new VertexiumException("Unexpected has value type " + has.value.getClass().getName());
                        }
                        break;
                    default:
                        throw new VertexiumException("Unexpected GeoCompare predicate " + has.predicate);
                }
            } else {
                throw new VertexiumException("Unexpected predicate type " + has.predicate.getClass().getName());
            }
        }
        return filters;
    }

    protected void addElementTypeFilter(List<FilterBuilder> filters, String elementType) {
        filters.add(createElementTypeFilter(elementType));
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
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());
    }

    protected AndFilterBuilder getFilterBuilder(List<FilterBuilder> filters) {
        return FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
    }

    private String[] splitStringIntoTerms(String value) {
        String[] values = value.split("[ -]");
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].trim();
        }
        return values;
    }

    protected QueryBuilder createQuery(QueryParameters queryParameters, String elementType, List<FilterBuilder> filters) {
        QueryBuilder query;
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
}
