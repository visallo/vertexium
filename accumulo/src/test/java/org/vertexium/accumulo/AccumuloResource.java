package org.vertexium.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.rules.ExternalResource;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.util.DataInDataTableStreamingPropertyValueStorageStrategy;
import org.vertexium.inmemory.search.DefaultSearchIndex;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.vertexium.test.GraphTestBase.*;

public class AccumuloResource extends ExternalResource {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloResource.class);

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "test";

    private File tempDir;
    private MiniAccumuloCluster accumulo;

    private Map extraConfig = null;

    public AccumuloResource() {
    }

    public AccumuloResource(Map extraConfig) {
        this.extraConfig = extraConfig;
    }

    @Override
    protected void before() throws Throwable {
        ensureAccumuloIsStarted();
        super.before();
    }

    @Override
    protected void after() {
        try {
            stop();
        } catch (Exception e) {
            LOGGER.info("Unable to shut down mini accumulo cluster", e);
        }
        super.after();
    }

    public void dropGraph() throws Exception {
        Connector connector = createConnector();
        AccumuloGraphTestUtils.ensureTableExists(connector, GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getDataTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getVerticesTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getHistoryVerticesTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getEdgesTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getExtendedDataTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getHistoryEdgesTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getMetadataTableName(GraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        connector.securityOperations().changeUserAuthorizations(
            AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME,
            new org.apache.accumulo.core.security.Authorizations(
                VISIBILITY_A_STRING,
                VISIBILITY_B_STRING,
                VISIBILITY_C_STRING,
                VISIBILITY_MIXED_CASE_STRING
            )
        );
    }

    public void addAuthorizations(AccumuloGraph graph, String... authorizations) {
        try {
            String principal = graph.getConnector().whoami();
            Authorizations currentAuthorizations = graph.getConnector().securityOperations().getUserAuthorizations(principal);

            List<byte[]> newAuthorizationsArray = new ArrayList<>();
            for (byte[] currentAuth : currentAuthorizations) {
                newAuthorizationsArray.add(currentAuth);
            }

            for (String authorization : authorizations) {
                if (!currentAuthorizations.contains(authorization)) {
                    newAuthorizationsArray.add(authorization.getBytes(UTF_8));
                }
            }

            Authorizations newAuthorizations = new Authorizations(newAuthorizationsArray);
            graph.getConnector().securityOperations().changeUserAuthorizations(principal, newAuthorizations);
        } catch (Exception ex) {
            throw new VertexiumException("could not add authorizations", ex);
        }
    }

    public MiniAccumuloCluster getAccumulo() {
        return accumulo;
    }

    @SuppressWarnings("unchecked")
    public Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, false);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, GraphTestBase.LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        configMap.put(AccumuloGraphConfiguration.HISTORY_IN_SEPARATE_TABLE, true);
        configMap.put(AccumuloGraphConfiguration.STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX, DataInDataTableStreamingPropertyValueStorageStrategy.class.getName());
        configMap.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());

        if (extraConfig != null) {
            configMap.putAll(extraConfig);
        }

        return configMap;
    }

    public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        return new AccumuloGraphConfiguration(createConfig()).createConnector();
    }

    public void ensureAccumuloIsStarted() {
        try {
            start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }

    protected void stop() throws IOException, InterruptedException {
        if (accumulo != null) {
            LOGGER.info("Stopping accumulo");
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }

    public void start() throws IOException, InterruptedException {
        if (accumulo != null) {
            return;
        }

        LOGGER.info("Starting accumulo");

        tempDir = File.createTempFile("accumulo-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: %s", tempDir);

        MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempDir, ACCUMULO_PASSWORD);
        miniAccumuloConfig.setZooKeeperStartupTime(60000);
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    AccumuloResource.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });
    }
}
