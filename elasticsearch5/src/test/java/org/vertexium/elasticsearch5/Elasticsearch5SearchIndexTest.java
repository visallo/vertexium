package org.vertexium.elasticsearch5;

import org.junit.ClassRule;

public class Elasticsearch5SearchIndexTest extends Elasticsearch5SearchIndexTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource();

    @Override
    protected ElasticsearchResource getElasticsearchResource() {
        return elasticsearchResource;
    }

    @Override
    protected boolean isPainlessDateMath() {
        return true;
    }
}
