package org.vertexium.elasticsearch;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.index.search.stats.SearchStats;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.Vertex;
import org.vertexium.query.QueryResultsIterable;

import static org.junit.Assert.assertEquals;
import static org.vertexium.test.util.VertexiumAssert.assertResultsCount;

public class ElasticsearchSingleDocumentSearchIndexTest extends ElasticsearchSingleDocumentSearchIndexTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(ElasticsearchSingleDocumentSearchIndexTest.class.getName());

    @Override
    protected ElasticsearchResource getElasticsearchResource() {
        return elasticsearchResource;
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

    private long getNumQueries() {
        AdminClient adminClient = elasticsearchResource.getRunner().client().admin();
        NodesStatsResponse nodeStats = new NodesStatsRequestBuilder(adminClient.cluster()).get();

        NodeStats[] nodes = nodeStats.getNodes();
        assertEquals(1, nodes.length);

        SearchStats searchStats = nodes[0].getIndices().getSearch();
        return searchStats.getTotal().getQueryCount();
    }
}
