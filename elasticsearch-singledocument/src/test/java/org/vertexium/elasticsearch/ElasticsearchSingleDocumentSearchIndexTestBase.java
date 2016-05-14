package org.vertexium.elasticsearch;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;
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
import java.lang.reflect.Field;
import java.util.*;

import static org.vertexium.util.IterableUtils.count;

public abstract class ElasticsearchSingleDocumentSearchIndexTestBase extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchSingleDocumentSearchIndexTestBase.class);
    public static final String ES_INDEX_NAME = "vertexium-test";
    private static File tempDir;
    private static Node elasticSearchNode;
    private static String addr;
    private static String clusterName;
    private static final boolean USE_REAL_ES = false;

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        tempDir = File.createTempFile("elasticsearch-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: %s", tempDir);

        clusterName = UUID.randomUUID().toString();
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(GroovyPlugin.class);
        elasticSearchNode = new TestNode(
                Settings.settingsBuilder()
                        .put("name", clusterName)
                        .put("cluster.name", clusterName)
                        .put("node.local", "false")
                        .put("script.inline", "true")
                        .put("index.number_of_shards", "1")
                        .put("index.number_of_replicas", "0")
                        .put("path.home", tempDir.getAbsolutePath())
                        .put("discovery.zen.ping.unicast.hosts", "localhost")
                        .build(),
                classpathPlugins);
        elasticSearchNode.start();
    }

    private static class TestNode extends Node {
        public TestNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(new Environment(preparedSettings), Version.CURRENT, classpathPlugins);
        }
    }

    @Before
    @Override
    public void before() throws Exception {
        if (elasticSearchNode.client().admin().indices().prepareExists(ES_INDEX_NAME).execute().actionGet().isExists()) {
            LOGGER.info("deleting test index: %s", ES_INDEX_NAME);
            elasticSearchNode.client().admin().indices().prepareDelete(ES_INDEX_NAME).execute().actionGet();
        }

        ClusterStateResponse response = elasticSearchNode.client().admin().cluster().prepareState().execute().actionGet();
        addr = response.getState().getNodes().getNodes().values().iterator().next().value.getAddress().toString();

        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        if (elasticSearchNode != null) {
            elasticSearchNode.close();
        }
        FileUtils.deleteDirectory(tempDir);
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
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.SCORING_STRATEGY_CLASS_NAME, EdgeCountScoringStrategy.class.getName());
        if (USE_REAL_ES) {
            addr = "localhost";
        } else {
            config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CLUSTER_NAME, clusterName);
        }
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.ES_LOCATIONS, addr);
        config.putAll(additionalConfiguration);
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(config);
        return InMemoryGraph.create(configuration);
    }

    private ElasticsearchSingleDocumentSearchIndex getSearchIndex() {
        return (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        ElasticsearchSingleDocumentSearchIndex searchIndex = (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();
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

    protected boolean isFieldNamesInQuerySupported() {
        return false;
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

