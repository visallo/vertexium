package org.vertexium.elasticsearch;

import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.Visibility;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.vertexium.elasticsearch.DistributedMetadataTablePropertyNameVisibilitiesStore.NODE_PATH;

public class DistributedMetadataTablePropertyNameVisibilitiesStoreTest {
    private static final String VIZ_X = "vizX";
    private static final String VIZ_Y = "vizY";
    private static final Authorizations VIZ_XY_AUTHS = new InMemoryAuthorizations(VIZ_X, VIZ_Y);
    private static final String PROP_NAME_A = "propA";

    private Graph graph;
    private DistributedMetadataTablePropertyNameVisibilitiesStore visibilitiesStore1;
    private DistributedMetadataTablePropertyNameVisibilitiesStore visibilitiesStore2;

    @ClassRule
    public static ZookeeperResource zookeeperResource = new ZookeeperResource();

    @Before
    public void before() throws Exception {
        CuratorFramework curator = zookeeperResource.getApacheCuratorFramework();
        if (curator.checkExists().forPath(NODE_PATH) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(NODE_PATH);
        }

        graph = InMemoryGraph.create();
        Map<String, String> config = new HashMap<>();
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexConfiguration.CONFIG_ES_LOCATIONS, "");
        config.put(ElasticSearchSearchIndexConfiguration.ZOOKEEPER_SERVERS, zookeeperResource.getZookeeperTestingServer().getConnectString());
        GraphConfiguration gConfig = new GraphConfiguration(config);
        visibilitiesStore1 = new DistributedMetadataTablePropertyNameVisibilitiesStore(graph, gConfig);
        visibilitiesStore2 = new DistributedMetadataTablePropertyNameVisibilitiesStore(graph, gConfig);
    }

    @After
    public void after() throws Exception {
        visibilitiesStore1.close();
        visibilitiesStore2.close();
    }

    @Test
    public void getHashForNewPropertiesShouldUpdateDistributedStores() throws Exception {
        String hash1 = visibilitiesStore1.getHash(graph, PROP_NAME_A, new Visibility(VIZ_X));
        String hash2 = visibilitiesStore2.getHash(graph, PROP_NAME_A, new Visibility(VIZ_Y));

        Collection<String> hashes1 = visibilitiesStore1.getHashes(graph, PROP_NAME_A, VIZ_XY_AUTHS);
        Collection<String> hashes2 = visibilitiesStore2.getHashes(graph, PROP_NAME_A, VIZ_XY_AUTHS);

        assertTrue(hashes2.contains(hash1));
        assertTrue(hashes2.contains(hash2));

        assertTrue(hashes1.contains(hash1));
        assertTrue(hashes1.contains(hash2)); // this fails if the distribution fails
    }
}
