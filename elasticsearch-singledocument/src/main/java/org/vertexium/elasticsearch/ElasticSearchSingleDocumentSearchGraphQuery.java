package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.GraphQuery;

import java.util.Map;

public class ElasticSearchSingleDocumentSearchGraphQuery extends ElasticSearchSingleDocumentSearchQueryBase implements GraphQuery {
    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String queryString,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, scoringStrategy, indexSelectionStrategy, pageSize, authorizations);
    }

    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, scoringStrategy, indexSelectionStrategy, pageSize, authorizations);
    }
}
