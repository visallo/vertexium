package org.vertexium.elasticsearch5;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.rules.ExternalResource;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.VertexiumException;
import org.vertexium.test.TestMetadataPlugin;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
    public static final int TEST_QUERY_PAGE_SIZE = 30;
    public static final int TEST_QUERY_PAGING_LIMIT = 50;

    private ElasticsearchClusterRunner runner;
    private String clusterName;
    private Client remoteClient;

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

        expandVertexiumPlugin(basePath);
        expandMapperSizePlugin(basePath);

        LogConfigurator.registerErrorListener();

        if (shouldUseRemoteElasticsearch()) {
            runner = null;
        } else {
            runner = new ElasticsearchClusterRunner();
            runner.onBuild((i, builder) ->
                builder.put("script.inline", "true")
                    .put("cluster.name", clusterName)
                    .put("http.type", "netty3")
                    .put("transport.type", "netty3")
            ).build(newConfigs().basePath(basePath.getAbsolutePath()).numOfNode(1));
            runner.ensureGreen();
        }
    }

    private boolean shouldUseRemoteElasticsearch() {
        return Strings.isNotEmpty(getRemoteEsAddresses());
    }

    private String getRemoteEsAddresses() {
        return System.getProperty("REMOTE_ES_ADDRESSES");
    }

    private void expandVertexiumPlugin(File basePath) {
        File vertexiumPluginDir = new File(basePath, "plugins/vertexium");
        vertexiumPluginDir.mkdirs();
        expandPlugin(vertexiumPluginDir, "vertexium-elasticsearch5-plugin.zip", "elasticsearch/plugin-descriptor.properties");
    }

    private void expandMapperSizePlugin(File basePath) {
        File mapperPluginDir = new File(basePath, "plugins/mapper-size");
        mapperPluginDir.mkdirs();
        expandPlugin(mapperPluginDir, "mapper-size-5.6.10.zip");
    }

    @SuppressWarnings("unchecked")
    private void expandPlugin(File pluginDir, String pluginZipName, String... filesToExtract) {
        InputStream zipIn = getClass().getResourceAsStream("/" + pluginZipName);
        File pluginZip = new File(pluginDir.getParentFile(), pluginZipName);
        try (OutputStream zipOut = new FileOutputStream(pluginZip)) {
            IOUtils.copy(zipIn, zipOut);
        } catch (Exception ex) {
            throw new VertexiumException("Could not write plugin zip file", ex);
        }
        try {
            ZipFile zipFile = new ZipFile(pluginZip);
            if (filesToExtract == null || filesToExtract.length == 0) {
                List<FileHeader> fileHeaders = zipFile.getFileHeaders();
                filesToExtract = fileHeaders.stream()
                    .filter(fileHeader -> !fileHeader.isDirectory())
                    .map(FileHeader::getFileName)
                    .toArray(String[]::new);
            }

            for (String s : filesToExtract) {
                String fileName = s.replace("elasticsearch/", "");
                zipFile.extractFile(s, pluginDir.getAbsolutePath(), null, fileName);
            }
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

    public Client getRemoteClient() {
        if (remoteClient == null) {
            Settings settings = Settings.builder()
                .put("cluster.name", System.getProperty("REMOTE_ES_CLUSTER_NAME", "elasticsearch"))
                .build();
            TransportAddress[] transportAddresses = Arrays.stream(getRemoteEsAddresses().split(","))
                .map(address -> {
                    String[] parts = address.split(":");
                    try {
                        InetAddress inetAddress = InetAddress.getByName(parts[0]);
                        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9300;
                        return new InetSocketTransportAddress(inetAddress, port);
                    } catch (Exception ex) {
                        throw new VertexiumException("cannot find host: " + address, ex);
                    }
                })
                .toArray(TransportAddress[]::new);
            remoteClient = new PreBuiltTransportClient(settings)
                .addTransportAddresses(transportAddresses);
        }
        return remoteClient;
    }

    public void dropIndices() throws Exception {
        AdminClient client = shouldUseRemoteElasticsearch()
            ? getRemoteClient().admin()
            : runner.admin();
        String[] indices = client.indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("deleting test index: %s", index);
                client.indices().prepareDelete(index).execute().actionGet();
            }
        }
    }

    public void clearIndices(Elasticsearch5SearchIndex searchIndex) throws Exception {
        String[] indices = searchIndex.getClient().admin().indices().prepareGetIndex().execute().get().indices();
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
        configMap.put(AUTO_FLUSH, false);
        configMap.put(GraphConfiguration.METADATA_PLUGIN, TestMetadataPlugin.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX, Elasticsearch5SearchIndex.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, ES_INDEX_NAME);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        if (shouldUseRemoteElasticsearch()) {
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, System.getProperty("REMOTE_ES_CLUSTER_NAME", "elasticsearch"));
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, System.getProperty("REMOTE_ES_ADDRESSES"));
        } else {
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, clusterName);
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, getLocation());
        }
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, Integer.parseInt(System.getProperty("ES_NUMBER_OF_SHARDS", "1")));
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, Integer.parseInt(System.getProperty("ES_NUMBER_OF_REPLICAS", "0")));
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ERROR_ON_MISSING_VERTEXIUM_PLUGIN, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ENABLE_SIZE_FIELD, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_SPLIT_EDGES_AND_VERTICES, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + LOG_REQUEST_SIZE_LIMIT, 10000);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGE_SIZE, TEST_QUERY_PAGE_SIZE);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGING_LIMIT, TEST_QUERY_PAGING_LIMIT);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + MAX_QUERY_STRING_TERMS, 20);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, TestElasticsearch5ExceptionHandler.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_REFRESH_INTERVAL, "30s");
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + REFRESH_INDEX_ON_FLUSH, false);

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
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_EDGES, "false");
        return true;
    }

    public ElasticsearchClusterRunner getRunner() {
        return runner;
    }
}
