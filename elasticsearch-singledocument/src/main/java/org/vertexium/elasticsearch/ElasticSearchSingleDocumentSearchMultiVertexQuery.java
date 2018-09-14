package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.MultiVertexQuery;

public class ElasticSearchSingleDocumentSearchMultiVertexQuery extends ElasticSearchSingleDocumentSearchGraphQuery implements MultiVertexQuery {
    public ElasticSearchSingleDocumentSearchMultiVertexQuery(
            Client client,
            Graph graph,
            String[] vertexIds,
            String queryString,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            int termAggregationShardSize,
            int termAggregationSize,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, scoringStrategy, indexSelectionStrategy, pageSize, termAggregationShardSize, termAggregationSize, authorizations);
        hasId(vertexIds);
    }

    public ElasticSearchSingleDocumentSearchMultiVertexQuery(
            Client client,
            Graph graph,
            String[] vertexIds,
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
        hasId(vertexIds);
    }
}