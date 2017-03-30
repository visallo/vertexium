package org.vertexium.elasticsearch;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.rules.ExternalResource;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.vertexium.GraphConfiguration.AUTO_FLUSH;
import static org.vertexium.GraphConfiguration.SEARCH_INDEX_PROP_PREFIX;
import static org.vertexium.elasticsearch.DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX;
import static org.vertexium.elasticsearch.DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME;
import static org.vertexium.elasticsearch.ElasticSearchSearchIndexConfiguration.*;

public class ElasticsearchResource extends ExternalResource {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchResource.class);

    private static final String ES_INDEX_NAME = "vertexium-test";
    private static final String ES_CLUSTER_NAME = "vertexium-test-cluster";
    private static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium-test-";
    private static final String PLUGIN_CLASS_PATH = "/vertexium-elasticsearch-singledocument-plugin.zip";

    private ElasticsearchClusterRunner runner;

    private Map extraConfig = null;

    public ElasticsearchResource() {
    }

    public ElasticsearchResource(Map extraConfig) {
        this.extraConfig = extraConfig;
    }

    @Override
    protected void before() throws Throwable {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File basePath = new File(tempDir, "vertexium-test-" + UUID.randomUUID().toString());

        installPlugin(basePath);

        runner = new ElasticsearchClusterRunner();
        runner.onBuild((i, builder) ->
                builder.put("script.disable_dynamic", "false")
                        .put("gateway.type", "local")
                        .put("index.number_of_shards", "1")
                        .put("cluster.name", ES_CLUSTER_NAME)
                        .put("index.number_of_replicas", "0")
        ).build(newConfigs().basePath(basePath.getAbsolutePath()).ramIndexStore().numOfNode(1));

        runner.ensureGreen();
        super.before();
    }

    @Override
    protected void after() {
        if (runner != null) {
            runner.close();
            runner.clean();
        }
        super.after();
    }

    public void dropIndices() throws Exception {
        String[] indices = runner.admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.equals(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("deleting test index: %s", index);
                runner.admin().indices().prepareDelete(index).execute().actionGet();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AUTO_FLUSH, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX, ElasticsearchSingleDocumentSearchIndex.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, ES_INDEX_NAME);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + STORE_SOURCE_DATA, "true");
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, ES_CLUSTER_NAME);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, getLocation());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, 1);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, 0);

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

    private void installPlugin(File basePath) throws IOException, ZipException {
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

    public boolean disableEdgeIndexing(Graph graph) {
        ElasticsearchSingleDocumentSearchIndex searchIndex = (ElasticsearchSingleDocumentSearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.INDEX_EDGES, "false");
        return true;
    }

}
