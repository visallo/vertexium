package org.vertexium.elasticsearch2;

import org.junit.ClassRule;

public class Elasticsearch2SearchIndexTest extends Elasticsearch2SearchIndexTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource();

    @Override
    protected ElasticsearchResource getElasticsearchResource() {
        return elasticsearchResource;
    }
}
