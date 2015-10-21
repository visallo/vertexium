package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ElasticSearchSingleDocumentSearchQueryBase extends ElasticSearchQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation,
        GraphQueryWithStatisticsAggregation {
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
        return false;
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType, int skip, int limit, boolean includeAggregations) {
        SearchRequestBuilder searchRequestBuilder = super.getSearchRequestBuilder(filters, queryBuilder, elementType, skip, limit, includeAggregations);
        if (includeAggregations) {
            List<AbstractAggregationBuilder> aggs = getElasticsearchAggregations(getAggregations());
            for (AbstractAggregationBuilder aggregationBuilder : aggs) {
                searchRequestBuilder.addAggregation(aggregationBuilder);
            }
        }
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
                Collection<String> fields = es.getQueryableElementTypeVisibilityPropertyNames(getGraph(), getParameters().getAuthorizations());
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
                continue;
            }
            if (!propertyDefinition.isSortable()) {
                throw new VertexiumException("Cannot sort on non-sortable fields");
            }
            q.addSort(propertyDefinition.getPropertyName(), esOrder);
        }
    }
}

