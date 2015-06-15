package org.vertexium.accumulo;

import com.google.common.base.Joiner;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.*;
import org.vertexium.id.SimpleNameSubstitutionStrategy;
import org.vertexium.id.SimpleSubstitutionUtils;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class AccumuloSimpleSubstitutionGraphTest extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloSimpleSubstitutionGraphTest.class);
    private final String ACCUMULO_USERNAME = "root";
    private final String ACCUMULO_PASSWORD = "test";
    private File tempDir;
    private static MiniAccumuloCluster accumulo;

    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        return AccumuloGraph.create(createConfiguration());
    }

    protected Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        return createConfiguration().createConnector();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        ensureAccumuloIsStarted();
        Connector connector = createConnector();
        AccumuloGraphTestUtils.ensureTableExists(connector, AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getDataTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getVerticesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getEdgesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        AccumuloGraphTestUtils.dropGraph(connector, AccumuloGraph.getMetadataTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        connector.securityOperations().changeUserAuthorizations(
                AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME,
                new org.apache.accumulo.core.security.Authorizations(
                        VISIBILITY_A_STRING,
                        VISIBILITY_B_STRING,
                        VISIBILITY_C_STRING,
                        VISIBILITY_MIXED_CASE_STRING
                )
        );
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return false;
    }

    @Test
    public void testFilterHints() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n1", "value1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        e1.addPropertyValue("k1", "n1", "value1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v2, v1, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.NONE, AUTHORIZATIONS_A);
        assertNotNull(v1);
        Assert.assertEquals(0, IterableUtils.count(v1.getProperties()));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", FetchHint.ALL, AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(1, IterableUtils.count(v1.getProperties()));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", EnumSet.of(FetchHint.PROPERTIES), AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(1, IterableUtils.count(v1.getProperties()));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", FetchHint.EDGE_REFS, AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(0, IterableUtils.count(v1.getProperties()));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", EnumSet.of(FetchHint.IN_EDGE_REFS), AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(0, IterableUtils.count(v1.getProperties()));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", EnumSet.of(FetchHint.OUT_EDGE_REFS), AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(0, IterableUtils.count(v1.getProperties()));
        assertEquals(0, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        e1 = graph.getEdge("e1", FetchHint.NONE, AUTHORIZATIONS_A);
        assertNotNull(e1);
        assertEquals(0, IterableUtils.count(e1.getProperties()));
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));

        e1 = graph.getEdge("e1", FetchHint.ALL, AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1.getProperties()));
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));

        e1 = graph.getEdge("e1", EnumSet.of(FetchHint.PROPERTIES), AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1.getProperties()));
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));
    }

    @Test
    public void testStoringEmptyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        Metadata metadata = new Metadata();
        v1.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        metadata = new Metadata();
        metadata.add("meta1", "metavalue1", VISIBILITY_EMPTY);
        v2.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().entrySet().size());

        v2 = graph.getVertex("v2", AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.entrySet().size());
        assertEquals("metavalue1", metadata.getEntry("meta1", VISIBILITY_EMPTY).getValue());

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        Scanner vertexScanner = accumuloGraph.createVertexScanner(FetchHint.ALL, AccumuloGraph.SINGLE_VERSION, null, null, AUTHORIZATIONS_EMPTY);
        vertexScanner.setRange(new Range("V", "W"));
        RowIterator rows = new RowIterator(vertexScanner.iterator());
        while (rows.hasNext()) {
            Iterator<Map.Entry<Key, Value>> row = rows.next();
            while (row.hasNext()) {
                Map.Entry<Key, Value> col = row.next();
                if (col.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                    if (col.getKey().getRow().toString().equals("Vv1")) {
                        assertEquals("", col.getValue().toString());
                    } else if (col.getKey().getRow().toString().equals("Vv2")) {
                        assertNotEquals("", col.getValue().toString());
                    } else {
                        fail("invalid vertex");
                    }
                }
            }
        }
    }

    public void start() throws IOException, InterruptedException {
        if (accumulo != null) {
            return;
        }

        LOGGER.info("Starting accumulo");

        tempDir = File.createTempFile("blueprints-accumulo-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: %s", tempDir);

        MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempDir, ACCUMULO_PASSWORD);
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    AccumuloSimpleSubstitutionGraphTest.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });
    }

    protected Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, true);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, GraphTestBase.LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.KEY_IDENTIFIER}), "k1");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), "k");
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        return configMap;
    }

    protected void stop() throws IOException, InterruptedException {
        if (accumulo != null) {
            LOGGER.info("Stopping accumulo");
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }

    protected void ensureAccumuloIsStarted() {
        try {
            start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }

    private AccumuloGraphConfiguration createConfiguration() {
        return new AccumuloGraphConfiguration(createConfig());
    }
}
