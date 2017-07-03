package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.query.GraphQuery;

public class ElasticSearchSingleDocumentSearchGraphQuery extends ElasticSearchSingleDocumentSearchQueryBase implements GraphQuery {
    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String queryString,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, options, authorizations);
    }

    public ElasticSearchSingleDocumentSearchGraphQuery(
            Client client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, options, authorizations);
    }
}
