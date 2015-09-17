package org.vertexium.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.KeyBase;
import org.vertexium.accumulo.iterator.model.VertexiumInvalidKeyException;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.vertexium.util.IterableUtils.toList;


public abstract class AccumuloGraphTestBase extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraphTestBase.class);
    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "test";
    private File tempDir;
    private static MiniAccumuloCluster accumulo;

    @ClassRule
    public static ZookeeperResource zookeeperResource = new ZookeeperResource();

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

        final String path = AccumuloGraphConfiguration.DEFAULT_ZOOKEEPER_METADATA_SYNC_PATH;
        CuratorFramework curator = zookeeperResource.getApacheCuratorFramework();
        if (curator.checkExists().forPath(path) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        }

        super.before();
    }

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

    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return false;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
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

    @SuppressWarnings("unchecked")
    protected Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, false);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, GraphTestBase.LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        return configMap;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
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
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    AccumuloGraphTestBase.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });
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
        assertEquals(0, IterableUtils.count(v1.getProperties()));
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
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().entrySet().size());

        v2 = graph.getVertex("v2", AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.entrySet().size());
        assertEquals("metavalue1", metadata.getEntry("meta1", VISIBILITY_EMPTY).getValue());

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        ScannerBase vertexScanner = accumuloGraph.createVertexScanner(FetchHint.ALL, AccumuloGraph.SINGLE_VERSION, null, null, new Range("V", "W"), AUTHORIZATIONS_EMPTY);
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

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testGetKeyValuePairsForVertexMutation() {
        VertexBuilder m = graph.prepareVertex("v1", 100L, VISIBILITY_A);

        Metadata metadata = new Metadata();
        metadata.add("key1_prop2_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop2_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "author", "value_key1_author", metadata, 400L, VISIBILITY_A_AND_B);

        metadata = new Metadata();
        metadata.add("key1_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "prop1", "value_key1_prop1", metadata, 200L, VISIBILITY_A);

        metadata = new Metadata();
        metadata.add("key2_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key2_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key2", "prop1", "value_key2_prop1", metadata, 300L, VISIBILITY_B);

        List<KeyValuePair> keyValuePairs = toList(((VertexBuilderWithKeyValuePairs) m).getKeyValuePairs());
        Collections.sort(keyValuePairs);
        assertEquals(10, keyValuePairs.size());

        String authorDeflated = substitutionDeflate("author");

        int i = 0;
        KeyValuePair pair;

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY, new Text(authorDeflated + "\u001fkey1"), new Text("a&b"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key1_author"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY, new Text("prop1\u001fkey1"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key1_prop1"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY, new Text("prop1\u001fkey2"), new Text("b"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key2_prop1"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text(authorDeflated + "\u001fkey1\u001Fa&b\u001Fkey1_prop2_m1"), new Text("a"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text(authorDeflated + "\u001fkey1\u001Fa&b\u001Fkey1_prop2_m2"), new Text("a"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey1\u001Fa\u001Fkey1_prop1_m1"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey1\u001Fa\u001Fkey1_prop1_m2"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey2\u001Fb\u001Fkey2_prop1_m1"), new Text("a"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey2\u001Fb\u001Fkey2_prop1_m2"), new Text("a"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("v1"), AccumuloVertex.CF_SIGNAL, ElementMutationBuilder.EMPTY_TEXT, new Text("a"), 100L), pair.getKey());
        assertEquals(ElementMutationBuilder.EMPTY_VALUE, pair.getValue());
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testGetKeyValuePairsForEdgeMutation() {
        EdgeBuilderByVertexId m = graph.prepareEdge("e1", "v1", "v2", "label1", 100L, VISIBILITY_A);

        Metadata metadata = new Metadata();
        metadata.add("key1_prop2_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop2_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "author", "value_key1_author", metadata, 400L, VISIBILITY_A_AND_B);

        metadata = new Metadata();
        metadata.add("key1_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "prop1", "value_key1_prop1", metadata, 200L, VISIBILITY_A);

        metadata = new Metadata();
        metadata.add("key2_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key2_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key2", "prop1", "value_key2_prop1", metadata, 300L, VISIBILITY_B);

        List<KeyValuePair> keyValuePairs = toList(((EdgeBuilderWithKeyValuePairs) m).getEdgeTableKeyValuePairs());
        Collections.sort(keyValuePairs);
        assertEquals(12, keyValuePairs.size());

        String authorDeflated = substitutionDeflate("author");

        int i = 0;
        KeyValuePair pair;

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloEdge.CF_SIGNAL, new Text("label1"), new Text("a"), 100L), pair.getKey());
        assertEquals(ElementMutationBuilder.EMPTY_VALUE, pair.getValue());

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloEdge.CF_IN_VERTEX, new Text("v2"), new Text("a"), 100L), pair.getKey());
        assertEquals(ElementMutationBuilder.EMPTY_VALUE, pair.getValue());

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloEdge.CF_OUT_VERTEX, new Text("v1"), new Text("a"), 100L), pair.getKey());
        assertEquals(ElementMutationBuilder.EMPTY_VALUE, pair.getValue());

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY, new Text(authorDeflated + "\u001fkey1"), new Text("a&b"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key1_author"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY, new Text("prop1\u001fkey1"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key1_prop1"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY, new Text("prop1\u001fkey2"), new Text("b"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("value_key2_prop1"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text(authorDeflated + "\u001fkey1\u001Fa&b\u001Fkey1_prop2_m1"), new Text("a"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text(authorDeflated + "\u001fkey1\u001Fa&b\u001Fkey1_prop2_m2"), new Text("a"), 400L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey1\u001Fa\u001Fkey1_prop1_m1"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey1\u001Fa\u001Fkey1_prop1_m2"), new Text("a"), 200L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey2\u001Fb\u001Fkey2_prop1_m1"), new Text("a"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m1_value"));
        pair = keyValuePairs.get(i++);
        assertEquals(new Key(new Text("e1"), AccumuloElement.CF_PROPERTY_METADATA, new Text("prop1\u001fkey2\u001Fb\u001Fkey2_prop1_m2"), new Text("a"), 300L), pair.getKey());
        assertTrue(pair.getValue().toString().contains("m2_value"));

        i = 0;
        keyValuePairs = toList(((EdgeBuilderWithKeyValuePairs) m).getVertexTableKeyValuePairs());
        Collections.sort(keyValuePairs);
        assertEquals(2, keyValuePairs.size());

        pair = keyValuePairs.get(i++);
        org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = new EdgeInfo(getGraph().getNameSubstitutionStrategy().deflate("label1"), "v2");
        assertEquals(new Key(new Text("v1"), AccumuloVertex.CF_OUT_EDGE, new Text("e1"), new Text("a"), 100L), pair.getKey());
        assertEquals(edgeInfo.toValue(), pair.getValue());

        pair = keyValuePairs.get(i++);
        edgeInfo = new EdgeInfo(getGraph().getNameSubstitutionStrategy().deflate("label1"), "v1");
        assertEquals(new Key(new Text("v2"), AccumuloVertex.CF_IN_EDGE, new Text("e1"), new Text("a"), 100L), pair.getKey());
        assertEquals(edgeInfo.toValue(), pair.getValue());
    }

    protected abstract String substitutionDeflate(String str);

    @Test
    public void testPropertyWithValueSeparator() {
        try {
            graph.prepareVertex("v1", VISIBILITY_EMPTY)
                    .addPropertyValue("prop1" + KeyBase.VALUE_SEPARATOR, "name1", "test", VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_EMPTY);
            throw new RuntimeException("Should have thrown a bad character exception");
        } catch (VertexiumInvalidKeyException ex) {
            // ok
        }
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }
}
