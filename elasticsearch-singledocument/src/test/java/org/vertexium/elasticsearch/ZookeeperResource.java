package org.vertexium.elasticsearch;

import com.google.common.base.Throwables;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

import java.io.IOException;

public class ZookeeperResource extends ExternalResource {
    private TestingServer testingServer;
    private CuratorFramework curatorFramework;
    private RetryPolicy retryPolicy;

    public ZookeeperResource() {
        this(new RetryOneTime(250));
    }

    public ZookeeperResource(RetryPolicy curatorRetryPolicy) {
        this.retryPolicy = curatorRetryPolicy;
    }

    @Override
    protected void before() throws Exception {
        testingServer = new TestingServer();
        testingServer.start();
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), retryPolicy);
        curatorFramework.start();
    }

    @Override
    protected void after() {
        try {
            curatorFramework.close();
            testingServer.stop();
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    public TestingServer getZookeeperTestingServer() {
        return testingServer;
    }

    public CuratorFramework getApacheCuratorFramework() {
        return curatorFramework;
    }
}
