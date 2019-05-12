package org.vertexium.elasticsearch5;

import org.elasticsearch.client.Client;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.query.GraphQuery;

public class ElasticsearchSearchGraphQuery extends ElasticsearchSearchQueryBase implements GraphQuery {
    public ElasticsearchSearchGraphQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String queryString,
        Options options,
        Authorizations authorizations
    ) {
        super(
            client,
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            queryString,
            options,
            authorizations
        );
    }

    public ElasticsearchSearchGraphQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String[] similarToFields,
        String similarToText,
        Options options,
        Authorizations authorizations
    ) {
        super(
            client,
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            similarToFields,
            similarToText,
            options,
            authorizations
        );
    }
}
