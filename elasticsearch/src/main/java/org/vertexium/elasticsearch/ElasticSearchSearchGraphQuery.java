package org.vertexium.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.query.GraphQuery;

import java.util.Map;

public class ElasticSearchSearchGraphQuery extends ElasticSearchSearchQueryBase implements GraphQuery {
    public ElasticSearchSearchGraphQuery(
            TransportClient client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, indexSelectionStrategy, authorizations);
    }

    public ElasticSearchSearchGraphQuery(
            TransportClient client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, indexSelectionStrategy, authorizations);
    }
}