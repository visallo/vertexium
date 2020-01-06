package org.vertexium.elasticsearch7;

import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.junit.rules.ExternalResource;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.test.TestMetadataPlugin;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.vertexium.GraphConfiguration.AUTO_FLUSH;
import static org.vertexium.GraphConfiguration.SEARCH_INDEX_PROP_PREFIX;
import static org.vertexium.elasticsearch7.DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX;
import static org.vertexium.elasticsearch7.DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME;
import static org.vertexium.elasticsearch7.ElasticsearchSearchIndexConfiguration.*;

public class ElasticsearchResource extends ExternalResource {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchResource.class);

    private static final String ES_INDEX_NAME = "vertexium-test";
    private static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium-test-";
    public static final int TEST_QUERY_PAGING_LIMIT = 50;

    private ElasticsearchClusterRunner runner;
    private String clusterName;
    private static Client sharedClient;

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

        LogConfigurator.registerErrorListener();

        if (shouldUseRemoteElasticsearch()) {
            runner = null;
        } else {
            runner = new ElasticsearchClusterRunner();
            runner.onBuild((i, builder) ->
                builder.put("cluster.name", clusterName)
                    .put("http.type", "netty4")
                    .put("transport.type", "netty4")
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

    @Override
    protected void after() {
        if (sharedClient != null) {
            sharedClient.close();
            sharedClient = null;
        }
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
        AdminClient client = runner.admin();
        String[] indices = client.indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("deleting test index: %s", index);
                client.indices().prepareDelete(index).execute().actionGet();
            }
        }
    }

    public void clearIndices(Elasticsearch7SearchIndex searchIndex) throws Exception {
        String[] indices = searchIndex.getClient().admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("clearing test index: %s", index);
                BulkByScrollResponse response = new DeleteByQueryRequestBuilder(searchIndex.getClient(), DeleteByQueryAction.INSTANCE)
                    .source(index)
                    .filter(QueryBuilders.matchAllQuery())
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
        configMap.put(SEARCH_INDEX_PROP_PREFIX, Elasticsearch7SearchIndexWithSharedClient.class.getName());
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
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_SPLIT_EDGES_AND_VERTICES, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + LOG_REQUEST_SIZE_LIMIT, 10000);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGE_SIZE, 30);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGING_LIMIT, TEST_QUERY_PAGING_LIMIT);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + MAX_QUERY_STRING_TERMS, 20);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, TestElasticsearch7ExceptionHandler.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_REFRESH_INTERVAL, -1);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + REFRESH_INDEX_ON_FLUSH, false);

        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "transport.type", "netty4");
        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "http.type", "netty4");

        if (extraConfig != null) {
            configMap.putAll(extraConfig);
        }

        return configMap;
    }

    private String getLocation() {
        ClusterStateResponse responsee = runner.node().client().admin().cluster().prepareState().execute().actionGet();
        TransportAddress address = responsee.getState().getNodes().getNodes().values().iterator().next().value.getAddress();
        return "localhost:" + address.address().getPort();
    }

    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch7SearchIndex searchIndex = (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_EDGES, "false");
        return true;
    }

    public ElasticsearchClusterRunner getRunner() {
        return runner;
    }

    // transport-7.4.2.jar!/org/elasticsearch/transport/client/PreBuiltTransportClient.class:134 likes to sleep on
    // connection close if default or netty4. This speeds up the test by re-using the client to skip the close.
    public static class Elasticsearch7SearchIndexWithSharedClient extends Elasticsearch7SearchIndex {
        public Elasticsearch7SearchIndexWithSharedClient(Graph graph, GraphConfiguration config) {
            super(graph, config);
        }

        @Override
        protected Client createClient(ElasticsearchSearchIndexConfiguration config) {
            if (sharedClient == null) {
                sharedClient = super.createClient(config);
            }
            return sharedClient;
        }

        @Override
        protected void shutdownElasticsearchClient() {
        }
    }
}
