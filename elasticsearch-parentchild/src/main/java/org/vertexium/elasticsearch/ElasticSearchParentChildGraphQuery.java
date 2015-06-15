package org.vertexium.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.query.GraphQuery;

import java.util.Map;

public class ElasticSearchParentChildGraphQuery extends ElasticSearchParentChildQueryBase implements GraphQuery {
    public ElasticSearchParentChildGraphQuery(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, queryString, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, authorizations);
    }

    public ElasticSearchParentChildGraphQuery(
            TransportClient client,
            String[] indicesToQuery,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, authorizations);
    }
}
