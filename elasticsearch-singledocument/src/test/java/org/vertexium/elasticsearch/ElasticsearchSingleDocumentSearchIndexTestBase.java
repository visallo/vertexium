package org.vertexium.elasticsearch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.Vertex;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.query.SortDirection;
import org.vertexium.test.GraphTestBase;

import static org.vertexium.util.IterableUtils.count;

public abstract class ElasticsearchSingleDocumentSearchIndexTestBase extends GraphTestBase {

    @Before
    @Override
    public void before() throws Exception {
        getElasticsearchResource().dropIndices();
        super.before();
    }

    @Override
    protected Graph createGraph() throws Exception {
        return InMemoryGraph.create(new InMemoryGraphConfiguration(getElasticsearchResource().createConfig()));
    }

    protected abstract ElasticsearchResource getElasticsearchResource();

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Override
    protected void addAuthorizations(String... authorizations) {
        getGraph().createAuthorizations(authorizations);
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        return getElasticsearchResource().disableEdgeIndexing(graph);
    }

    private ElasticsearchSingleDocumentSearchIndex getSearchIndex() {
        return (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    protected boolean isAdvancedGeoQuerySupported() {
        return false;
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

