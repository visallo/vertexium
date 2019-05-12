package org.vertexium.elasticsearch5;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.elasticsearch5.scoring.ElasticsearchFieldValueScoringStrategy;
import org.vertexium.elasticsearch5.scoring.ElasticsearchHammingDistanceScoringStrategy;
import org.vertexium.elasticsearch5.sorting.ElasticsearchLengthOfStringSortingStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.sorting.SortingStrategy;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.vertexium.test.util.VertexiumAssert.assertResultsCount;
import static org.vertexium.util.IterableUtils.toList;

public class Elasticsearch5GraphTest extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(Elasticsearch5GraphTest.class);

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(Elasticsearch5GraphTest.class.getName());

    @Override
    public Elasticsearch5Graph getGraph() {
        return (Elasticsearch5Graph) super.getGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return getGraph().createAuthorizations(auths);
    }

    @Override
    protected Authorizations createButDontAddAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Override
    protected Graph createGraph() {
        Elasticsearch5GraphConfiguration configuration = new Elasticsearch5GraphConfiguration(elasticsearchResource.createConfig());
        return Elasticsearch5Graph.create(configuration);
    }

    private Elasticsearch5GraphSearchIndex getSearchIndex() {
        return (Elasticsearch5GraphSearchIndex) graph.getSearchIndex();
    }

    @Override
    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    @Override
    protected boolean isPartialUpdateOfVertexPropertyKeySupported() {
        return true;
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    protected boolean isPainlessDateMath() {
        return true;
    }

    @Override
    protected boolean isDefaultSearchIndex() {
        return false;
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
        assertEquals(startingNumQueries + 2, getNumQueries());

        vertices = graph.query(AUTHORIZATIONS_A).limit(1).vertices();
        assertEquals(startingNumQueries + 4, getNumQueries());

        assertResultsCount(1, 2, vertices);
        assertEquals(startingNumQueries + 4, getNumQueries());

        vertices = graph.query(AUTHORIZATIONS_A).limit(10).vertices();
        assertEquals(startingNumQueries + 6, getNumQueries());

        assertResultsCount(2, 2, vertices);
        assertEquals(startingNumQueries + 6, getNumQueries());
    }

    @Test
    public void testQueryExecutionCountGetEdges() {
        getGraph().prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", "label", VISIBILITY_A).save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);

        long startingNumQueries = getNumQueries();
        v1.getEdgesSummary(AUTHORIZATIONS_A);
        assertEquals(startingNumQueries + 4, getNumQueries()); // TODO should we store edge info on vertices

        startingNumQueries = getNumQueries();
        toList(v1.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(startingNumQueries + 2, getNumQueries());
    }

    @Test
    public void testQueryExecutionCountWhenScrollingApi() {
        getGraph().getConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + Elasticsearch5GraphConfiguration.QUERY_PAGE_SIZE, 1);

        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, vertices);

        long startingNumQueries = getNumQueries();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, vertices);
        assertEquals(startingNumQueries + 2, getNumQueries());

        getGraph().getConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + Elasticsearch5GraphConfiguration.QUERY_PAGE_SIZE, 2);

        graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        startingNumQueries = getNumQueries();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(3, vertices);
        assertEquals(startingNumQueries + 2, getNumQueries());
    }

    @Test
    public void testDisallowLeadingWildcardsInQueryString() {
        graph.prepareVertex("v1", VISIBILITY_A).setProperty("prop1", "value1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        try {
            graph.query("*alue1", AUTHORIZATIONS_A).search().getTotalHits();
            fail("Wildcard prefix of query string should have caused an exception");
        } catch (Exception e) {
            if (!(getRootCause(e) instanceof NotSerializableExceptionWrapper)) {
                fail("Wildcard prefix of query string should have caused a NotSerializableExceptionWrapper exception");
            }
        }
    }

    @Test
    public void testLimitingNumberOfQueryStringTerms() {
        graph.prepareVertex("v1", VISIBILITY_A).setProperty("prop1", "value1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        StringBuilder q = new StringBuilder();
        for (int i = 0; i < getGraph().getConfiguration().getMaxQueryStringTerms();
             i++) {
            q.append("jeff").append(i).append(" ");
        }

        // should succeed
        graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();

        try {
            q.append("done");
            graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();
            fail("Exceeding max query terms should have thrown an exception");
        } catch (VertexiumException e) {
            // expected
        }
    }

    @Test
    public void testQueryReturningElasticsearchEdge() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
            .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        Edge e1 = toList(edges).get(0);
        assertEquals(LABEL_LABEL1, e1.getLabel());
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));
        assertEquals("e1", e1.getId());
    }

    @Test
    public void testQueryReturningElasticsearchVertex() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_B)
            .vertices(FetchHints.NONE);

        assertResultsCount(1, 1, vertices);
        Vertex vertex = toList(vertices).get(0);
        assertEquals("v2", vertex.getId());
    }

    @Test
    public void testRetrievingVerticesFromElasticsearchEdge() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
            .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        toList(edges).get(0).getVertices(AUTHORIZATIONS_A);
    }

    private long getNumQueries() {
        Client client = elasticsearchResource.getRunner().client();
        NodesStatsResponse nodeStats = NodesStatsAction.INSTANCE.newRequestBuilder(client).get();

        List<NodeStats> nodes = nodeStats.getNodes();
        assertEquals(1, nodes.size());

        SearchStats searchStats = nodes.get(0).getIndices().getSearch();
        return searchStats.getTotal().getQueryCount();
    }

    private Throwable getRootCause(Throwable e) {
        if (e.getCause() == null) {
            return e;
        }
        return getRootCause(e.getCause());
    }

    @Override
    protected ScoringStrategy getHammingDistanceScoringStrategy(String field, String hash) {
        return new ElasticsearchHammingDistanceScoringStrategy(field, hash);
    }

    @Override
    protected SortingStrategy getLengthOfStringSortingStrategy(String propertyName) {
        return new ElasticsearchLengthOfStringSortingStrategy(propertyName);
    }

    @Override
    protected ScoringStrategy getFieldValueScoringStrategy(String field) {
        return new ElasticsearchFieldValueScoringStrategy(field);
    }

    @Override
    protected boolean isAddWithoutIndexingSupported() {
        return false;
    }

    @Override
    protected void printAll() {
        for (SourceAndFields sourceAndFields : getElementsInSearchIndex()) {
            print(sourceAndFields);
        }
    }

    private void print(SourceAndFields sourceAndFields) {
        System.out.println("-------------------------------------------------");
        System.out.println(sourceAndFields);
        System.out.println("  Source");
        for (Map.Entry<String, Object> sourceEntry : sourceAndFields.source.entrySet()) {
            System.out.println("    " + sourceEntry.getKey() + " = " + sourceEntry.getValue());
        }
        System.out.println("  Fields");
        for (Map.Entry<String, SearchHitField> fieldEntry : sourceAndFields.fields.entrySet()) {
            System.out.println("    " + fieldEntry.getKey() + " = " + fieldEntry.getValue());
        }
    }

    public SourceAndFields[] getElementsInSearchIndex() {
        LOGGER.debug("getElementsInSearchIndex");
        Client client = getGraph().getClient();
        Stream<SearchHit> stream = Arrays.stream(client
            .prepareSearch("vertexium-test-edges", "vertexium-test-vertices")
            .setFetchSource(true)
            .storedFields("*")
            .get()
            .getHits()
            .getHits());
        return stream.map(sh -> new SourceAndFields(sh.getSource(), sh.getFields())).toArray(SourceAndFields[]::new);
    }

    public static class SourceAndFields {
        private final Map<String, Object> source;
        private final Map<String, SearchHitField> fields;

        public SourceAndFields(Map<String, Object> source, Map<String, SearchHitField> fields) {
            this.source = source;
            this.fields = fields;
        }

        @Override
        public String toString() {
            return source.get(FieldNames.ELEMENT_TYPE) + ": " + source.get(FieldNames.ELEMENT_ID);
        }
    }
}
