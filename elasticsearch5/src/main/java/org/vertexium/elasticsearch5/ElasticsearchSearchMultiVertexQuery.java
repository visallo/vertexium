package org.vertexium.elasticsearch5;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.query.MultiVertexQuery;

public class ElasticsearchSearchMultiVertexQuery extends ElasticsearchSearchGraphQuery implements MultiVertexQuery {
    public ElasticsearchSearchMultiVertexQuery(
            Client client,
            Graph graph,
            String[] vertexIds,
            String queryString,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, options, authorizations);
        hasId(vertexIds);
    }

    public ElasticsearchSearchMultiVertexQuery(
            Client client,
            Graph graph,
            String[] vertexIds,
            String[] similarToFields,
            String similarToText,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, options, authorizations);
        hasId(vertexIds);
    }
}
