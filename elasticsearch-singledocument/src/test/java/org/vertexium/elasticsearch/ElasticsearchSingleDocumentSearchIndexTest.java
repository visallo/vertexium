package org.vertexium.elasticsearch;

import org.junit.ClassRule;

public class ElasticsearchSingleDocumentSearchIndexTest extends ElasticsearchSingleDocumentSearchIndexTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource();

    @Override
    protected ElasticsearchResource getElasticsearchResource() {
        return elasticsearchResource;
    }
}
