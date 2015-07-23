package org.vertexium.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class DistributedMetadataTablePropertyNameVisibilitiesStore extends MetadataTablePropertyNameVisibilitiesStore implements Closeable {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DistributedMetadataTablePropertyNameVisibilitiesStore.class);
    private static final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    @VisibleForTesting
    static final String NODE_PATH = "/vertexium/propertyNameVisibilitiesCache";

    private final CuratorFramework curator;
    private final NodeCache nodeCache;

    public DistributedMetadataTablePropertyNameVisibilitiesStore(Graph graph, GraphConfiguration config) {
        super(graph);
        ElasticSearchSearchIndexConfiguration esConfig = new ElasticSearchSearchIndexConfiguration(graph, config);
        curator = CuratorFrameworkFactory.newClient(esConfig.getZookeeperServers(), retryPolicy);
        curator.start();
        nodeCache = new NodeCache(curator, NODE_PATH);
        try {
            try {
                curator.create().creatingParentsIfNeeded().forPath(NODE_PATH);
            } catch (KeeperException.NodeExistsException e) {
                // no problem
            }
            nodeCache.start();
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    synchronized (DistributedMetadataTablePropertyNameVisibilitiesStore.this) {
                        LOGGER.debug(NODE_PATH + " changed - clearing hashes cache");
                        clearHashesCache();
                    }
                }
            });
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws IOException {
        nodeCache.close();
        curator.close();
    }

    @Override
    protected void onPropertyChanged(String propertyName, Hashes hashes) {
        try {
            curator.setData().forPath(NODE_PATH, new byte[0]);
        } catch (Exception e) {
            LOGGER.error("unable to set data on " + NODE_PATH, e);
        }
    }
}
