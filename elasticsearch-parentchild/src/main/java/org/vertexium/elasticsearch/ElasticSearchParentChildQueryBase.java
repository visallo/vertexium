package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.Contains;
import org.vertexium.query.QueryParameters;
import org.vertexium.query.QueryStringQueryParameters;
import org.vertexium.query.SimilarToTextQueryParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ElasticSearchParentChildQueryBase extends ElasticSearchQueryBase {
    protected ElasticSearchParentChildQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, queryString, propertyDefinitions, scoringStrategy, false, authorizations);
    }

    protected ElasticSearchParentChildQueryBase(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, false, authorizations);
    }

    @Override
    protected QueryBuilder createQuery(QueryParameters queryParameters, String elementType, List<FilterBuilder> filters) {
        FilterBuilder andFilterBuilder = getElementFilter(elementType);

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

    protected FilterBuilder getElementFilter(String elementType) {
        List<FilterBuilder> filters = getElementFilters(elementType);
        return FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
    }

    protected List<FilterBuilder> getElementFilters(String elementType) {
        List<FilterBuilder> results = new ArrayList<>();
        results.add(createElementTypeFilter(elementType));
        results.add(new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations()));
        return results;
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

    @Override
    protected void getFiltersForHasNotPropertyContainer(List<FilterBuilder> filters, HasNotPropertyContainer hasNotProperty) {
        throw new VertexiumNotSupportedException("hasNot cannot be performed in ES with parent/child indexing.");
    }

    @Override
    protected void getFiltersForContainsPredicate(List<FilterBuilder> filters, Contains contains, HasValueContainer has) {
        switch (contains) {
            case NOT_IN:
                throw new VertexiumNotSupportedException("NOT_IN cannot be performed in ES with parent/child indexing.");
            default:
                super.getFiltersForContainsPredicate(filters, contains, has);
                break;
        }
    }
}
