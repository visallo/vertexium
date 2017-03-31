package org.vertexium.elasticsearch2;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.query.SortDirection;
import org.vertexium.test.GraphTestBase;

import static org.vertexium.util.IterableUtils.count;

public abstract class Elasticsearch2SearchIndexTestBase extends GraphTestBase {

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        getElasticsearchResource().dropIndices();
        super.before();
    }

    protected abstract ElasticsearchResource getElasticsearchResource();

    @Override
    protected Graph createGraph() throws Exception {
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(getElasticsearchResource().createConfig());
        return InMemoryGraph.create(configuration);
    }

    private Elasticsearch2SearchIndex getSearchIndex() {
        return (Elasticsearch2SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    protected boolean isFieldNamesInQuerySupported() {
        return false;
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch2SearchIndex searchIndex = (Elasticsearch2SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.INDEX_EDGES, "false");
        return true;
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return true;
    }

    @Test
    @Override
    public void testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex() {
        super.testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex();

        getSearchIndex().clearIndexInfoCache();

        QueryResultsIterable<Vertex> vertices
                = graph.query(AUTHORIZATIONS_A).sort("age", SortDirection.ASCENDING).vertices();
        Assert.assertEquals(2, count(vertices));
    }
}

