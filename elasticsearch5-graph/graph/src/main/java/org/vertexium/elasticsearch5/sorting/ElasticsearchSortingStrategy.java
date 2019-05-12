package org.vertexium.elasticsearch5.sorting;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.vertexium.elasticsearch5.Elasticsearch5Graph;
import org.vertexium.query.QueryParameters;
import org.vertexium.query.SortDirection;
import org.vertexium.sorting.SortingStrategy;

public interface ElasticsearchSortingStrategy extends SortingStrategy {
    void updateElasticsearchQuery(
        Elasticsearch5Graph graph,
        SearchRequestBuilder q,
        QueryParameters parameters,
        SortDirection direction
    );
}
