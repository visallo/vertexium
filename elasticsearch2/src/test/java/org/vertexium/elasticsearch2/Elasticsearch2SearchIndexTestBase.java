package org.vertexium.elasticsearch2;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.*;
import org.vertexium.*;
import org.vertexium.elasticsearch2.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch2.score.EdgeCountScoringStrategyConfiguration;
import org.vertexium.elasticsearch2.score.ScoringStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.query.SortDirection;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.vertexium.util.IterableUtils.count;

public abstract class Elasticsearch2SearchIndexTestBase extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(Elasticsearch2SearchIndexTestBase.class);
    public static final String ES_INDEX_NAME = "vertexium-test";
    public static final String ES_CLUSTER_NAME = "vertexium-test-cluster";
    public static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium-test-";
    private static ElasticsearchClusterRunner runner;

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File basePath = new File(tempDir, "vertexium-test-" + UUID.randomUUID().toString());
        LOGGER.info("base path: %s", basePath);

        runner = new ElasticsearchClusterRunner();
        runner.onBuild((i, builder) ->
                               builder
                                       .put("script.inline", "true")
                                       .put("cluster.name", ES_CLUSTER_NAME)
                                       .put("plugin.types", "org.elasticsearch.script.groovy.GroovyPlugin")
        ).build(
                newConfigs()
                        .basePath(basePath.getAbsolutePath())
                        .numOfNode(1)
        );

        runner.ensureGreen();
    }

    @Before
    @Override
    public void before() throws Exception {
        String[] indices = runner.admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.equals(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("deleting test index: %s", index);
                runner.admin().indices().prepareDelete(index).execute().actionGet();
            }
        }
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        if (runner != null) {
            runner.close();
            runner.clean();
        }
    }

    @Override
    protected Graph createGraph() throws Exception {
        return createGraph(new HashMap());
    }

    protected Graph createGraph(Map additionalConfiguration) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(GraphConfiguration.AUTO_FLUSH, true);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, Elasticsearch2SearchIndex.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME, ES_INDEX_NAME);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.SCORING_STRATEGY_CLASS_NAME, EdgeCountScoringStrategy.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.CLUSTER_NAME, ES_CLUSTER_NAME);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.ES_LOCATIONS, getLocation());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.NUMBER_OF_SHARDS, 1);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.NUMBER_OF_REPLICAS, 0);
        config.putAll(additionalConfiguration);
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(config);
        return InMemoryGraph.create(configuration);
    }

    private String getLocation() {
        ClusterStateResponse responsee = runner.node().client().admin().cluster().prepareState().execute().actionGet();
        InetSocketTransportAddress address = (InetSocketTransportAddress)
                responsee.getState().getNodes().getNodes().values().iterator().next().value.getAddress();
        return "localhost:" + address.address().getPort();
    }

    private Elasticsearch2SearchIndex getSearchIndex() {
        return (Elasticsearch2SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        Elasticsearch2SearchIndex searchIndex = (Elasticsearch2SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        ElasticsearchSearchIndexConfiguration config = searchIndex.getConfig();
        ScoringStrategy scoringStrategy = config.getScoringStrategy();
        if (!(scoringStrategy instanceof EdgeCountScoringStrategy)) {
            return false;
        }

        EdgeCountScoringStrategyConfiguration edgeCountScoringStrategyConfig = ((EdgeCountScoringStrategy) scoringStrategy).getConfig();

        try {
            Field updateEdgeBoostField = edgeCountScoringStrategyConfig.getClass().getDeclaredField("updateEdgeBoost");
            updateEdgeBoostField.setAccessible(true);
            updateEdgeBoostField.set(edgeCountScoringStrategyConfig, false);
        } catch (Exception e) {
            throw new VertexiumException("Failed to update 'updateEdgeBoost' field", e);
        }

        return true;
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

