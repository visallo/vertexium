package org.vertexium.elasticsearch2;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.elasticsearch2.score.ScoringStrategy;
import org.vertexium.query.GraphQuery;

public class ElasticsearchSearchGraphQuery extends ElasticsearchSearchQueryBase implements GraphQuery {
    public ElasticsearchSearchGraphQuery(
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

    public ElasticsearchSearchGraphQuery(
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
