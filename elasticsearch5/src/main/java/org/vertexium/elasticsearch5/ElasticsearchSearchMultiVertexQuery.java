package org.vertexium.elasticsearch5;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.query.MultiVertexQuery;

public class ElasticsearchSearchMultiVertexQuery extends ElasticsearchSearchGraphQuery implements MultiVertexQuery {
    public ElasticsearchSearchMultiVertexQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String queryString,
        String[] vertexIds,
        Options options,
        Authorizations authorizations
    ) {
        super(client, graph, indexService, propertyNameService, propertyNameVisibilitiesStore, queryString, options, authorizations);
        hasId(vertexIds);
    }

    public ElasticsearchSearchMultiVertexQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String[] similarToFields,
        String similarToText,
        String[] vertexIds,
        Options options,
        Authorizations authorizations
    ) {
        super(client, graph, indexService, propertyNameService, propertyNameVisibilitiesStore, similarToFields, similarToText, options, authorizations);
        hasId(vertexIds);
    }
}
