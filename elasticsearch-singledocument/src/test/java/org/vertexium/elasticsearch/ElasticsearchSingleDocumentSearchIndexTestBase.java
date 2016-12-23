package org.vertexium.elasticsearch;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.*;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategyConfiguration;
import org.vertexium.elasticsearch.score.ScoringStrategy;
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
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.vertexium.util.IterableUtils.count;

public abstract class ElasticsearchSingleDocumentSearchIndexTestBase extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchSingleDocumentSearchIndexTestBase.class);
    public static final String ES_INDEX_NAME = "vertexium-test";
    public static final String ES_CLUSTER_NAME = "vertexium-test-cluster";
    public static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium-test-";
    private static final String PLUGIN_CLASS_PATH = "/vertexium-elasticsearch-singledocument-plugin.zip";
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
        installPlugin(basePath);

        runner = new ElasticsearchClusterRunner();
        runner.onBuild((i, builder) ->
                builder
                        .put("script.disable_dynamic", "false")
                        .put("gateway.type", "local")
                        .put("index.number_of_shards", "1")
                        .put("cluster.name", ES_CLUSTER_NAME)
                        .put("index.number_of_replicas", "0")
        ).build(newConfigs().basePath(basePath.getAbsolutePath()).ramIndexStore().numOfNode(1));

        runner.ensureGreen();
    }

    private static void installPlugin(File basePath) throws IOException, ZipException {
        File pluginsPath = new File(basePath, "plugins");
        File vertexiumZipFile = new File(basePath, "vertexium.zip");
        File vertexiumPluginPath = new File(pluginsPath, "vertexium-elasticsearch");
        if (!vertexiumPluginPath.mkdirs()) {
            System.out.println("Could not create directories");
        }
        InputStream in = ElasticsearchSingleDocumentSearchIndexTestBase.class.getResourceAsStream(PLUGIN_CLASS_PATH);
        if (in == null) {
            throw new VertexiumException("Could not find: " + PLUGIN_CLASS_PATH);
        }
        FileUtils.copyInputStreamToFile(in, vertexiumZipFile);
        ZipFile zipFile = new ZipFile(vertexiumZipFile);
        zipFile.extractAll(vertexiumPluginPath.getAbsolutePath());
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
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, ElasticsearchSingleDocumentSearchIndex.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME, ES_INDEX_NAME);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.STORE_SOURCE_DATA, "true");
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.SCORING_STRATEGY_CLASS_NAME, EdgeCountScoringStrategy.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CLUSTER_NAME, ES_CLUSTER_NAME);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.ES_LOCATIONS, getLocation());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.NUMBER_OF_SHARDS, 1);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.NUMBER_OF_REPLICAS, 0);
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

    private ElasticsearchSingleDocumentSearchIndex getSearchIndex() {
        return (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        ElasticsearchSingleDocumentSearchIndex searchIndex = (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        ElasticSearchSearchIndexConfiguration config = searchIndex.getConfig();
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

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        ElasticsearchSingleDocumentSearchIndex searchIndex = (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.INDEX_EDGES, "false");
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

