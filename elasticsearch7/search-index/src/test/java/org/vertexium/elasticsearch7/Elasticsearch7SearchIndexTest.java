package org.vertexium.elasticsearch7;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.search.stats.SearchStats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.elasticsearch7.lucene.DefaultQueryStringTransformer;
import org.vertexium.elasticsearch7.scoring.ElasticsearchFieldValueScoringStrategy;
import org.vertexium.elasticsearch7.scoring.ElasticsearchHammingDistanceScoringStrategy;
import org.vertexium.elasticsearch7.sorting.ElasticsearchLengthOfStringSortingStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.query.SortDirection;
import org.vertexium.query.TermsAggregation;
import org.vertexium.query.TermsResult;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.sorting.SortingStrategy;
import org.vertexium.test.GraphTestBase;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.vertexium.test.util.VertexiumAssert.assertResultsCount;
import static org.vertexium.util.CloseableUtils.closeQuietly;
import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;

public class Elasticsearch7SearchIndexTest extends GraphTestBase {

    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(Elasticsearch7SearchIndexTest.class.getName());

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
    @SuppressWarnings("unchecked")
    protected Graph createGraph() {
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(elasticsearchResource.createConfig());
        return InMemoryGraph.create(configuration);
    }

    private Elasticsearch7SearchIndex getSearchIndex() {
        return (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch7SearchIndex searchIndex = (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
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

    @Test
    public void testGraphQueryAggregateOnPropertyThatHasNoValuesInTheIndex() {
        super.testGraphQueryAggregateOnPropertyThatHasNoValuesInTheIndex();

        getSearchIndex().clearCache();

        TermsAggregation aliasAggregation = new TermsAggregation("alias-agg", "alias");
        aliasAggregation.setIncludeHasNotCount(true);
        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
            .addAggregation(aliasAggregation)
            .limit(0)
            .vertices();

        Assert.assertEquals(0, count(vertices));

        TermsResult aliasAggResult = vertices.getAggregationResult(aliasAggregation.getAggregationName(), TermsResult.class);
        assertEquals(2, aliasAggResult.getHasNotCount());
        assertEquals(0, count(aliasAggResult.getBuckets()));
    }

    @Override
    protected boolean isPainlessDateMath() {
        return true;
    }

    @Test
    public void testCustomQueryStringTransformer() {
        Elasticsearch7SearchIndex searchIndex = (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().setQueryStringTransformer(new DefaultQueryStringTransformer(graph) {
            @Override
            protected String[] expandFieldName(String fieldName, Authorizations authorizations) {
                if ("knownAs".equals(fieldName)) {
                    fieldName = "name";
                }
                return super.expandFieldName(fieldName, authorizations);
            }
        });

        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("food").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A)
            .setProperty("name", "Joe Ferner", VISIBILITY_A)
            .setProperty("food", "pizza", VISIBILITY_A)
            .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
            .setProperty("name", "Joe Smith", VISIBILITY_A)
            .setProperty("food", "salad", VISIBILITY_A)
            .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query("joe", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query("\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("name:\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("knownAs:\"joe ferner\"", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("knownAs:joe", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query("knownAs:joe", AUTHORIZATIONS_A).has("food", "pizza").vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("food:pizza", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("eats:pizza", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(0, count(vertices));
    }

    @Test
    public void testQueryExecutionCountWhenPaging() {
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
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
    public void testQueryExecutionCountWhenScrollingApi() {
        Elasticsearch7SearchIndex searchIndex = (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 1);

        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        long startingNumQueries = getNumQueries();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, vertices);
        assertEquals(startingNumQueries + 4, getNumQueries());

        searchIndex = (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 2);

        graph.prepareVertex("v3", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(3, vertices);
        assertEquals(startingNumQueries + 8, getNumQueries());
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
        for (int i = 0; i < getSearchIndex().getConfig().getMaxQueryStringTerms(); i++) {
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
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
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
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_B).save(AUTHORIZATIONS_B);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_B)
            .vertices(FetchHints.NONE);

        assertResultsCount(1, 1, vertices);
        Vertex vertex = toList(vertices).get(0);
        assertEquals("v2", vertex.getId());
    }

    @Test(expected = VertexiumNotSupportedException.class)
    public void testRetrievingVerticesFromElasticsearchEdge() {
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
            .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        toList(edges).get(0).getVertices(AUTHORIZATIONS_A);
    }

    @Test
    public void testUpdateVertexWithDeletedElasticsearchDocument() {
        TestElasticsearch7ExceptionHandler.authorizations = AUTHORIZATIONS_A;

        graph.prepareVertex("v1", VISIBILITY_A)
            .addPropertyValue("k1", "prop1", "joe", VISIBILITY_A)
            .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        getSearchIndex().deleteElement(graph, v1, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
            .addPropertyValue("k1", "prop2", "bob", VISIBILITY_A)
            .save(AUTHORIZATIONS_A);
        graph.flush();

        // Missing documents are treated as new documents (see BulkUpdateService#handleFailure) and thus are not part
        // of the initial flush.
        graph.flush();

        List<String> results = toList(graph.query("joe", AUTHORIZATIONS_A).vertexIds());
        assertEquals(1, results.size());
        assertEquals("v1", results.get(0));

        results = toList(graph.query("bob", AUTHORIZATIONS_A).vertexIds());
        assertEquals(1, results.size());
        assertEquals("v1", results.get(0));
    }

    @Test
    public void testQueryPagingVsScrollApi() {
        for (int i = 0; i < ElasticsearchResource.TEST_QUERY_PAGING_LIMIT * 2; i++) {
            graph.prepareVertex("v" + i, VISIBILITY_A)
                .addPropertyValue("k1", "prop1", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        }
        graph.flush();

        int resultCount = count(graph.query(AUTHORIZATIONS_A)
            .limit(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT - 1)
            .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT - 1, resultCount);

        resultCount = count(graph.query(AUTHORIZATIONS_A)
            .limit(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT + 1)
            .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT + 1, resultCount);

        resultCount = count(graph.query(AUTHORIZATIONS_A)
            .vertices());
        assertEquals(ElasticsearchResource.TEST_QUERY_PAGING_LIMIT * 2, resultCount);
    }

    @Test
    public void testMultipleThreadsFlushing() throws InterruptedException {
        AtomicBoolean startSignal = new AtomicBoolean();
        AtomicBoolean run = new AtomicBoolean(true);
        AtomicBoolean writing = new AtomicBoolean(false);
        AtomicBoolean writeThenFlushComplete = new AtomicBoolean(false);
        CountDownLatch threadsReadyCountdown = new CountDownLatch(2);
        Runnable waitForStart = () -> {
            try {
                while (!startSignal.get()) {
                    synchronized (startSignal) {
                        threadsReadyCountdown.countDown();
                        startSignal.wait();
                    }
                }
            } catch (Exception ex) {
                throw new VertexiumException("thread failed", ex);
            }
        };

        Thread constantWriteThread = new Thread(() -> {
            waitForStart.run();

            int i = 0;
            while (run.get()) {
                graph.prepareVertex("v" + i, new Visibility(""))
                    .addPropertyValue("k1", "name1", "value1", new Visibility(""))
                    .save(AUTHORIZATIONS_ALL);
                writing.set(true);
                i++;
            }
        });

        Thread writeThenFlushThread = new Thread(() -> {
            try {
                waitForStart.run();
                while (!writing.get()) {
                    Thread.sleep(10); // wait for other thread to start
                }

                for (int i = 0; i < 5; i++) {
                    graph.prepareVertex("vWriteTheFlush", new Visibility(""))
                        .addPropertyValue("k1", "name1", "value1", new Visibility(""))
                        .save(AUTHORIZATIONS_ALL);
                    graph.flush();
                }
                writeThenFlushComplete.set(true);
            } catch (Exception ex) {
                throw new VertexiumException("thread failed", ex);
            }
        });

        // synchronize thread start
        constantWriteThread.start();
        writeThenFlushThread.start();
        threadsReadyCountdown.await();
        Thread.sleep(100);
        synchronized (startSignal) {
            startSignal.set(true);
            startSignal.notifyAll();
        }

        // wait to finish
        int timeout = 5000;
        long startTime = System.currentTimeMillis();
        while (!writeThenFlushComplete.get() && (System.currentTimeMillis() - startTime < timeout)) {
            Thread.sleep(10);
        }
        long endTime = System.currentTimeMillis();
        run.set(false);
        constantWriteThread.join();
        writeThenFlushThread.join();

        // check results
        if (endTime - startTime > timeout) {
            fail("timeout waiting for flush");
        }
    }

    @Test
    public void testManyWritesToSameElement() throws InterruptedException {
        int threadCount = 10;
        int numberOfTimerToWrite = 100;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int write = 0; write < numberOfTimerToWrite; write++) {
                    String keyAndValue = Thread.currentThread().getId() + "-" + write;
                    getGraph().prepareVertex("v1", VISIBILITY_EMPTY)
                        .addPropertyValue(keyAndValue, "name", keyAndValue, VISIBILITY_EMPTY)
                        .save(AUTHORIZATIONS_EMPTY);
                    getGraph().flush();
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(threadCount * numberOfTimerToWrite, count(v1.getProperties("name")));
    }

    @Test
    public void testManyWritesToSameElementNoFlushTillEnd() throws InterruptedException {
        int threadCount = 5;
        int numberOfTimerToWrite = 20;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int write = 0; write < numberOfTimerToWrite; write++) {
                    String keyAndValue = Thread.currentThread().getId() + "-" + write;
                    getGraph().prepareVertex("v1", VISIBILITY_EMPTY)
                        .addPropertyValue(keyAndValue, "name", keyAndValue, VISIBILITY_EMPTY)
                        .save(AUTHORIZATIONS_EMPTY);
                }
                getGraph().flush();
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(threadCount * numberOfTimerToWrite, count(v1.getProperties("name")));
    }

    @Test
    public void testUnclosedScrollApi() {
        int verticesToCreate = ElasticsearchResource.TEST_QUERY_PAGE_SIZE * 2;
        for (int i = 0; i < verticesToCreate; i++) {
            getGraph().prepareVertex("v" + i, VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "value1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(AUTHORIZATIONS_EMPTY)
            .has("name", "value1")
            .limit((Long) null)
            .vertices();
        assertEquals(verticesToCreate, vertices.getTotalHits());
        Iterator<Vertex> it = vertices.iterator();
        assertTrue(it.hasNext());
        it.next();
        it = null;

        System.gc();
        System.gc();
    }

    @Test
    public void testLargeTotalHitsPaged() throws InterruptedException {
        int vertexCount = 10_045;
        for (int write = 0; write < vertexCount; write++) {
            getGraph().prepareVertex("v" + write, VISIBILITY_EMPTY).save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();
        QueryResultsIterable<String> queryResults = getGraph()
            .query("*", AUTHORIZATIONS_EMPTY)
            .limit(0)
            .vertexIds();
        assertEquals(vertexCount, queryResults.getTotalHits());
        closeQuietly(queryResults);
    }

    @Test
    public void testLargeTotalHitsScroll() throws InterruptedException {
        int vertexCount = 10_045;
        for (int write = 0; write < vertexCount; write++) {
            getGraph().prepareVertex("v" + write, VISIBILITY_EMPTY).save(AUTHORIZATIONS_EMPTY);
        }
        getGraph().flush();
        QueryResultsIterable<String> queryResults = getGraph()
            .query("*", AUTHORIZATIONS_EMPTY)
            .limit((Long) null)
            .vertexIds();
        assertEquals(vertexCount, queryResults.getTotalHits());
        closeQuietly(queryResults);
    }

    private long getNumQueries() {
        NodesStatsResponse nodeStats = new NodesStatsRequestBuilder(getSearchIndex().getClient(), NodesStatsAction.INSTANCE).get();

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
    protected boolean multivalueGeopointQueryWithinMeansAny() {
        return false;
    }
}
