package org.neolumin.vertexium.elasticsearch.helpers;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.GraphConfiguration;
import org.neolumin.vertexium.elasticsearch.ElasticSearchSearchIndex;
import org.neolumin.vertexium.elasticsearch.ElasticSearchSearchIndexConfiguration;
import org.neolumin.vertexium.inmemory.InMemoryGraph;
import org.neolumin.vertexium.inmemory.InMemoryGraphConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticSearchSearchIndexTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndexTestHelpers.class);
    public static final String ES_INDEX_NAME = "vertexium-test";
    private static File tempDir;
    private static Node elasticSearchNode;
    private static String addr;
    private static String clusterName;
    private static boolean TESTING = false;

    public static Graph createGraph() {
        Map config = new HashMap();
        config.put(GraphConfiguration.AUTO_FLUSH, true);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, ElasticSearchSearchIndex.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CONFIG_INDEX_NAME, ES_INDEX_NAME);
        if (TESTING) {
            addr = "localhost";
            config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CONFIG_STORE_SOURCE_DATA, "true");
        } else {
            config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CONFIG_CLUSTER_NAME, clusterName);
        }
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CONFIG_ES_LOCATIONS, addr);
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(config);
        return InMemoryGraph.create(configuration, configuration.createIdGenerator(), configuration.createSearchIndex());
    }

    public static void beforeClass() throws IOException {
        tempDir = File.createTempFile("elasticsearch-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: " + tempDir);

        clusterName = UUID.randomUUID().toString();
        elasticSearchNode = NodeBuilder
                .nodeBuilder()
                .local(false)
                .clusterName(clusterName)
                .settings(
                        ImmutableSettings.settingsBuilder()
                                .put("gateway.type", "local")
                                .put("index.number_of_shards", "1")
                                .put("index.number_of_replicas", "0")
                                .put("path.data", new File(tempDir, "data").getAbsolutePath())
                                .put("path.logs", new File(tempDir, "logs").getAbsolutePath())
                                .put("path.work", new File(tempDir, "work").getAbsolutePath())
                ).node();
        elasticSearchNode.start();
    }

    public static void before() {
        if (elasticSearchNode.client().admin().indices().prepareExists(ES_INDEX_NAME).execute().actionGet().isExists()) {
            LOGGER.info("deleting test index: " + ES_INDEX_NAME);
            elasticSearchNode.client().admin().indices().prepareDelete(ES_INDEX_NAME).execute().actionGet();
        }

        ClusterStateResponse response = elasticSearchNode.client().admin().cluster().prepareState().execute().actionGet();
        addr = response.getState().getNodes().getNodes().values().iterator().next().value.getAddress().toString();
        addr = addr.substring("inet[/".length());
        addr = addr.substring(0, addr.length() - 1);
    }

    public static void after() throws IOException {

    }

    public static void afterClass() throws IOException {
        if (elasticSearchNode != null) {
            elasticSearchNode.stop();
            elasticSearchNode.close();
        }
        FileUtils.deleteDirectory(tempDir);
    }
}
