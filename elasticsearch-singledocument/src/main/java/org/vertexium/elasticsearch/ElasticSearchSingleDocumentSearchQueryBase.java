package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ElasticSearchSingleDocumentSearchQueryBase extends ElasticSearchQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation,
        GraphQueryWithStatisticsAggregation {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<>();
    private final List<TermsQueryItem> termsQueryItems = new ArrayList<>();
    private final List<GeohashQueryItem> geohashQueryItems = new ArrayList<>();
    private final List<StatisticsQueryItem> statisticsQueryItems = new ArrayList<>();

    public ElasticSearchSingleDocumentSearchQueryBase(
            Client client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, indexSelectionStrategy, false, true, false, authorizations);
    }

    public ElasticSearchSingleDocumentSearchQueryBase(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, indexSelectionStrategy, false, true, false, authorizations);
    }

    @Override
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval, Long minDocumentCount) {
        histogramQueryItems.add(new HistogramQueryItem(aggregationName, fieldName, interval, minDocumentCount));
        return this;
    }

    @Override
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval) {
        return addHistogramAggregation(aggregationName, fieldName, interval, null);
    }

    @Override
    public GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String fieldName) {
        termsQueryItems.add(new TermsQueryItem(aggregationName, fieldName));
        return this;
    }

    @Override
    public GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String fieldName, int precision) {
        geohashQueryItems.add(new GeohashQueryItem(aggregationName, fieldName, precision));
        return this;
    }

    @Override
    public GraphQueryWithStatisticsAggregation addStatisticsAggregation(String aggregationName, String field) {
        statisticsQueryItems.add(new StatisticsQueryItem(aggregationName, field));
        return this;
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType, int skip, int limit) {
        SearchRequestBuilder searchRequestBuilder = super.getSearchRequestBuilder(filters, queryBuilder, elementType, skip, limit);
        addHistogramQueryToSearchRequestBuilder(searchRequestBuilder, histogramQueryItems);
        addTermsQueryToSearchRequestBuilder(searchRequestBuilder, termsQueryItems);
        addGeohashQueryToSearchRequestBuilder(searchRequestBuilder, geohashQueryItems);
        addStatisticsQueryToSearchRequestBuilder(searchRequestBuilder, statisticsQueryItems);
        return searchRequestBuilder;
    }

    @Override
    protected QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters) {
        String queryString = queryParameters.getQueryString();
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }
        ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex();
        Collection<String> fields = es.getQueryablePropertyNames(getGraph(), false, getParameters().getAuthorizations());
        QueryStringQueryBuilder qs = QueryBuilders.queryString(queryString);
        for (String field : fields) {
            qs = qs.field(field);
        }
        return qs;
    }

    @Override
    protected List<FilterBuilder> getFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> results = super.getFilters(elementType);
        if (getParameters() instanceof QueryStringQueryParameters) {
            String queryString = ((QueryStringQueryParameters) getParameters()).getQueryString();
            if (queryString == null || queryString.equals("*")) {
                ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex();
                Collection<String> fields = es.getQueryablePropertyNames(getGraph(), true, getParameters().getAuthorizations());
                OrFilterBuilder atLeastOneFieldExistsFilter = new OrFilterBuilder();
                for (String field : fields) {
                    atLeastOneFieldExistsFilter.add(new ExistsFilterBuilder(field));
                }
                results.add(atLeastOneFieldExistsFilter);
            }
        }
        return results;
    }

    @Override
    protected void applySort(SearchRequestBuilder q) {
        for (SortContainer sortContainer : getParameters().getSortContainers()) {
            SortOrder esOrder = sortContainer.direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
            PropertyDefinition propertyDefinition = getSearchIndex().getPropertyDefinition(getGraph(), sortContainer.propertyName);
            if (propertyDefinition == null) {
                throw new VertexiumException("Could not find property definition for field: " + sortContainer.propertyName);
            }
            if (!propertyDefinition.isSortable()) {
                throw new VertexiumException("Cannot sort on non-sortable fields");
            }
            q.addSort(propertyDefinition.getPropertyName(), esOrder);
        }
    }
}

