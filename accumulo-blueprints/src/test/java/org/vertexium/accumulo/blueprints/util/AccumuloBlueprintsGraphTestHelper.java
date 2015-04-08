package org.vertexium.accumulo.blueprints.util;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.apache.accumulo.core.client.Connector;
import org.junit.Ignore;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.blueprints.AccumuloAuthorizationsProvider;
import org.vertexium.accumulo.blueprints.AccumuloVertexiumBlueprintsGraph;
import org.vertexium.blueprints.AuthorizationsProvider;
import org.vertexium.blueprints.DefaultVisibilityProvider;
import org.vertexium.blueprints.VisibilityProvider;

import java.util.HashMap;

@Ignore
public class AccumuloBlueprintsGraphTestHelper extends GraphTest {
    private final AccumuloVertexiumBlueprintsGraph defaultGraph;
    private final VisibilityProvider visibilityProvider = new DefaultVisibilityProvider(new HashMap());
    private final AuthorizationsProvider authorizationsProvider = new AccumuloAuthorizationsProvider(new HashMap());

    public AccumuloBlueprintsGraphTestHelper() {
        try {
            this.ensureAccumuloIsStarted();
            AccumuloGraphConfiguration config = this.getGraphConfig(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
            this.defaultGraph = new AccumuloVertexiumBlueprintsGraph(AccumuloGraph.create(config), visibilityProvider, authorizationsProvider);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Graph generateGraph() {
        return this.defaultGraph;
    }

    @Override
    public Graph generateGraph(String graphDirectoryName) {
        try {
            AccumuloGraphConfiguration config = this.getGraphConfig(graphDirectoryName);
            return new AccumuloVertexiumBlueprintsGraph(AccumuloGraph.create(config), visibilityProvider, authorizationsProvider);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        throw new RuntimeException("not implemented");
    }

    public void setUp() {
        dropGraph(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
    }

    @Override
    public void dropGraph(String graphDirectoryName) {
        try {
            Connector connector = getConnector(graphDirectoryName);
            if (connector.tableOperations().exists(graphDirectoryName + "_d")) {
                connector.tableOperations().delete(graphDirectoryName + "_d");
            }
            if (connector.tableOperations().exists(graphDirectoryName + "_v")) {
                connector.tableOperations().delete(graphDirectoryName + "_v");
            }
            if (connector.tableOperations().exists(graphDirectoryName + "_e")) {
                connector.tableOperations().delete(graphDirectoryName + "_e");
            }
            connector.tableOperations().create(graphDirectoryName + "_d");
            connector.tableOperations().create(graphDirectoryName + "_v");
            connector.tableOperations().create(graphDirectoryName + "_e");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Connector getConnector(String graphDirectoryName) {
        try {
            return this.getGraphConfig(graphDirectoryName).createConnector();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureAccumuloIsStarted() {
        try {
            TestAccumuloCluster.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }

    private AccumuloGraphConfiguration getGraphConfig(String tableName) {
        AccumuloGraphConfiguration config = TestAccumuloCluster.getConfig();
        config.set(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX, tableName);
        return config;
    }
}
