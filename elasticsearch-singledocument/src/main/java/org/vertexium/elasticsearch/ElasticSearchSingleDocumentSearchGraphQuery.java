package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.GraphQuery;

public class ElasticSearchSingleDocumentSearchGraphQuery extends ElasticSearchSingleDocumentSearchQueryBase implements GraphQuery {
    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String queryString,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            int termAggregationShardSize,
            int termAggregationSize,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, scoringStrategy, indexSelectionStrategy, pageSize, termAggregationShardSize, termAggregationSize, authorizations);
    }

    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            int termAggregationShardSize,
            int termAggregationSize,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, scoringStrategy, indexSelectionStrategy, pageSize, termAggregationShardSize, termAggregationSize, authorizations);
    }
}
