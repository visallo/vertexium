package org.vertexium.elasticsearch5;

import net.lingala.zip4j.core.ZipFile;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.junit.rules.ExternalResource;
import org.vertexium.Graph;
import org.vertexium.VertexiumException;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.vertexium.GraphConfiguration.AUTO_FLUSH;
import static org.vertexium.GraphConfiguration.SEARCH_INDEX_PROP_PREFIX;
import static org.vertexium.elasticsearch5.DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX;
import static org.vertexium.elasticsearch5.DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME;
import static org.vertexium.elasticsearch5.ElasticsearchSearchIndexConfiguration.*;

public class ElasticsearchResource extends ExternalResource {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchResource.class);

    private static final String ES_INDEX_NAME = "vertexium-test";
    private static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium-test-";

    private ElasticsearchClusterRunner runner;
    private String clusterName;

    private Map extraConfig = null;

    public ElasticsearchResource(String clusterName) {
        this.clusterName = clusterName;
    }

    public ElasticsearchResource(String clusterName, Map extraConfig) {
        this.clusterName = clusterName;
        this.extraConfig = extraConfig;
    }

    @Override
    protected void before() throws Throwable {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File basePath = new File(tempDir, "vertexium-test-" + UUID.randomUUID().toString());
        LOGGER.info("base path: %s", basePath);

        File vertexiumPluginDir = new File(basePath, "plugins/vertexium");
        vertexiumPluginDir.mkdirs();
        expandVertexiumPlugin(vertexiumPluginDir);

        LogConfigurator.registerErrorListener();

        runner = new ElasticsearchClusterRunner();
        runner.onBuild((i, builder) ->
            builder.put("script.inline", "true")
                .put("cluster.name", clusterName)
                .put("http.type", "netty3")
                .put("transport.type", "netty3")
        ).build(newConfigs().basePath(basePath.getAbsolutePath()).numOfNode(1));

        runner.ensureGreen();
    }

    private void expandVertexiumPlugin(File vertexiumPluginDir) {
        InputStream zipIn = getClass().getResourceAsStream("/vertexium-elasticsearch5-plugin.zip");
        File pluginZip = new File(vertexiumPluginDir.getParentFile(), "vertexium-elasticsearch5-plugin.zip");
        try (OutputStream zipOut = new FileOutputStream(pluginZip)) {
            IOUtils.copy(zipIn, zipOut);
        } catch (Exception ex) {
            throw new VertexiumException("Could not write plugin zip file", ex);
        }
        try {
            ZipFile zipFile = new ZipFile(pluginZip);
            zipFile.extractFile("elasticsearch/plugin-descriptor.properties", vertexiumPluginDir.getAbsolutePath(), null, "plugin-descriptor.properties");
        } catch (Exception ex) {
            throw new VertexiumException("Could not extract plugin", ex);
        }
        pluginZip.delete();
    }

    @Override
    protected void after() {
        if (runner != null) {
            try {
                runner.close();
            } catch (IOException ex) {
                LOGGER.error("could not close runner", ex);
            }
            runner.clean();
        }
    }

    public void dropIndices() throws Exception {
        String[] indices = runner.admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("deleting test index: %s", index);
                runner.admin().indices().prepareDelete(index).execute().actionGet();
            }
        }
    }

    public void clearIndices(Elasticsearch5SearchIndex searchIndex) throws Exception {
        String[] indices = runner.admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("clearing test index: %s", index);
                BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(searchIndex.getClient())
                    .source(index)
                    .get();
                LOGGER.info("removed %d documents", response.getDeleted());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AUTO_FLUSH, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX, Elasticsearch5SearchIndex.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, ES_INDEX_NAME);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, clusterName);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, getLocation());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, 1);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, 0);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ERROR_ON_MISSING_VERTEXIUM_PLUGIN, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_SPLIT_EDGES_AND_VERTICES, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + LOG_REQUEST_SIZE_LIMIT, 10000);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + MAX_QUERY_STRING_TERMS, 20);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, TestElasticsearch5ExceptionHandler.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_REFRESH_INTERVAL, -1);

        // transport-5.3.3.jar!/org/elasticsearch/transport/client/PreBuiltTransportClient.class:61 likes to sleep on
        // connection close if default or netty4. This speeds up the test by skipping that
        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "transport.type", "netty3");
        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "http.type", "netty3");

        if (extraConfig != null) {
            configMap.putAll(extraConfig);
        }

        return configMap;
    }

    private String getLocation() {
        ClusterStateResponse responsee = runner.node().client().admin().cluster().prepareState().execute().actionGet();
        InetSocketTransportAddress address = (InetSocketTransportAddress)
            responsee.getState().getNodes().getNodes().values().iterator().next().value.getAddress();
        return "localhost:" + address.address().getPort();
    }

    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) graph.getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_EDGES, "false");
        return true;
    }

    public ElasticsearchClusterRunner getRunner() {
        return runner;
    }
}
