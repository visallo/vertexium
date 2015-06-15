package org.vertexium.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.GraphQuery;

import java.util.Map;

public class ElasticSearchSearchGraphQuery extends ElasticSearchSearchQueryBase implements GraphQuery {
    public ElasticSearchSearchGraphQuery(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, queryString, propertyDefinitions, scoringStrategy, authorizations);
    }

    public ElasticSearchSearchGraphQuery(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, authorizations);
    }
}