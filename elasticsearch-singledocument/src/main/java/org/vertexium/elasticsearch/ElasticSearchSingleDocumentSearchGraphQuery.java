package org.vertexium.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.GraphQuery;

import java.util.Map;

public class ElasticSearchSingleDocumentSearchGraphQuery extends ElasticSearchSingleDocumentSearchQueryBase implements GraphQuery {
    public ElasticSearchSingleDocumentSearchGraphQuery(
            TransportClient client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, indexSelectionStrategy, authorizations);
    }

    public ElasticSearchSingleDocumentSearchGraphQuery(
            TransportClient client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, indexSelectionStrategy, authorizations);
    }
}
