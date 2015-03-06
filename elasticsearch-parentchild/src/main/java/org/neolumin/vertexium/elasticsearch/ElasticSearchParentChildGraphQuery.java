package org.neolumin.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.PropertyDefinition;
import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.elasticsearch.score.ScoringStrategy;
import org.neolumin.vertexium.query.QueryParameters;
import org.neolumin.vertexium.query.QueryStringQueryParameters;
import org.neolumin.vertexium.query.SimilarToTextQueryParameters;

import java.util.List;
import java.util.Map;

public class ElasticSearchParentChildGraphQuery extends ElasticSearchGraphQueryBase {
    protected ElasticSearchParentChildGraphQuery(TransportClient client, String[] indicesToQuery, Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, ScoringStrategy scoringStrategy, Authorizations authorizations) {
        super(client, indicesToQuery, graph, queryString, propertyDefinitions, scoringStrategy, false, authorizations);
    }

    protected ElasticSearchParentChildGraphQuery(TransportClient client, String[] indicesToQuery, Graph graph, String[] similarToFields, String similarToText, Map<String, PropertyDefinition> propertyDefinitions, ScoringStrategy scoringStrategy, Authorizations authorizations) {
        super(client, indicesToQuery, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, false, authorizations);
    }

    @Override
    protected QueryBuilder createQuery(QueryParameters queryParameters, String elementType, List<FilterBuilder> filters) {
        FilterBuilder elementTypeFilter = createElementTypeFilter(elementType);
        AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter(
                elementTypeFilter,
                new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations())
        );

        AuthorizationFilterBuilder authorizationFilterBuilder = new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations());

        QueryBuilder childQuery;
        if (queryParameters instanceof QueryStringQueryParameters) {
            childQuery = createQueryStringQuery((QueryStringQueryParameters) queryParameters, elementType, filters, authorizationFilterBuilder);
        } else if (queryParameters instanceof SimilarToTextQueryParameters) {
            childQuery = createSimilarToTextQuery((SimilarToTextQueryParameters) queryParameters, elementType, filters, authorizationFilterBuilder);
        } else {
            throw new VertexiumException("Query parameters not supported of type: " + queryParameters.getClass().getName());
        }

        return QueryBuilders.filteredQuery(childQuery, andFilterBuilder);
    }

    private QueryBuilder createSimilarToTextQuery(SimilarToTextQueryParameters queryParameters, String elementType, List<FilterBuilder> filters, AuthorizationFilterBuilder authorizationFilterBuilder) {
        BoolQueryBuilder boolChildQuery = QueryBuilders.boolQuery();

        boolChildQuery.must(
                new HasChildQueryBuilder(ElasticSearchParentChildSearchIndex.PROPERTY_TYPE,
                        QueryBuilders.filteredQuery(
                                super.createQuery(queryParameters, elementType, filters),
                                authorizationFilterBuilder
                        )
                ).scoreType("avg")
        );

        addFiltersToQuery(boolChildQuery, filters, authorizationFilterBuilder);

        return boolChildQuery;
    }

    private QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters, String elementType, List<FilterBuilder> filters, AuthorizationFilterBuilder authorizationFilterBuilder) {
        String queryString = queryParameters.getQueryString();
        if (((queryString == null || queryString.length() <= 0)) && (filters.size() <= 0)) {
            return QueryBuilders.matchAllQuery();
        }

        BoolQueryBuilder boolChildQuery = QueryBuilders.boolQuery();

        if (queryString != null && queryString.length() > 0) {
            boolChildQuery.must(
                    new HasChildQueryBuilder(ElasticSearchParentChildSearchIndex.PROPERTY_TYPE,
                            QueryBuilders.filteredQuery(
                                    super.createQuery(queryParameters, elementType, filters),
                                    authorizationFilterBuilder
                            )
                    ).scoreType("avg")
            );
        }

        addFiltersToQuery(boolChildQuery, filters, authorizationFilterBuilder);

        return boolChildQuery;
    }

    private void addFiltersToQuery(BoolQueryBuilder boolChildQuery, List<FilterBuilder> filters, AuthorizationFilterBuilder authorizationFilterBuilder) {
        for (FilterBuilder filterBuilder : filters) {
            boolChildQuery.must(
                    new HasChildQueryBuilder(ElasticSearchParentChildSearchIndex.PROPERTY_TYPE,
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    FilterBuilders.andFilter(authorizationFilterBuilder, filterBuilder)
                            )
                    ).scoreType("avg")
            );
        }
    }

    @Override
    protected void addElementTypeFilter(List<FilterBuilder> filters, String elementType) {
        // don't add the element type filter here because the child docs don't have element type only the parent type does
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder) {
        return getClient()
                .prepareSearch(getIndicesToQuery())
                .setTypes(ElasticSearchSearchIndexBase.ELEMENT_TYPE)
                .setQuery(queryBuilder)
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());
    }

    @Override
    protected void addNotFilter(List<FilterBuilder> filters, String key, Object value) {
        filters.add(
                FilterBuilders.andFilter(
                        FilterBuilders.existsFilter(key),
                        FilterBuilders.notFilter(FilterBuilders.inFilter(key, value))
                )
        );
    }
}
