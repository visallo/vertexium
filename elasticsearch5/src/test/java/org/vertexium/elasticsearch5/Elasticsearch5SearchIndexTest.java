package org.vertexium.elasticsearch5;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.search.stats.SearchStats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.query.SortDirection;
import org.vertexium.test.GraphTestBase;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.vertexium.test.util.VertexiumAssert.assertResultsCount;
import static org.vertexium.util.IterableUtils.count;

public class Elasticsearch5SearchIndexTest extends GraphTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(Elasticsearch5SearchIndexTest.class.getName());

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Override
    protected void addAuthorizations(String... authorizations) {
        getGraph().createAuthorizations(authorizations);
    }

    @Before
    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Override
    protected Graph createGraph() throws Exception {
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(elasticsearchResource.createConfig());
        return InMemoryGraph.create(configuration);
    }

    private Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
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

        getSearchIndex().clearCache();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).sort("age", SortDirection.ASCENDING).vertices();
        Assert.assertEquals(2, count(vertices));
    }

    @Override
    protected boolean isPainlessDateMath() {
        return true;
    }

    @Test
    public void testQueryExecutionCountWhenPaging() {
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        long startingNumQueries = getNumQueries();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertEquals(startingNumQueries, getNumQueries());

        assertResultsCount(2, 2, vertices);
        assertEquals(startingNumQueries + 1, getNumQueries());

        vertices = graph.query(AUTHORIZATIONS_A).limit(1).vertices();
        assertEquals(startingNumQueries + 2, getNumQueries());

        assertResultsCount(1, 2, vertices);
        assertEquals(startingNumQueries + 2, getNumQueries());

        vertices = graph.query(AUTHORIZATIONS_A).limit(10).vertices();
        assertEquals(startingNumQueries + 3, getNumQueries());

        assertResultsCount(2, 2, vertices);
        assertEquals(startingNumQueries + 3, getNumQueries());
    }

    @Test
    public void testQueryExecutionCountWhenScrollingApi() {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 1);

        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        long startingNumQueries = getNumQueries();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, vertices);
        assertEquals(startingNumQueries + 2, getNumQueries());

        searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 2);

        graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(3, vertices);
        assertEquals(startingNumQueries + 4, getNumQueries());
    }

    private long getNumQueries() {
        Client client = elasticsearchResource.getRunner().client();
        NodesStatsResponse nodeStats = NodesStatsAction.INSTANCE.newRequestBuilder(client).get();

        List<NodeStats> nodes = nodeStats.getNodes();
        assertEquals(1, nodes.size());

        SearchStats searchStats = nodes.get(0).getIndices().getSearch();
        return searchStats.getTotal().getQueryCount();
    }
}
