package org.vertexium.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.VertexiumInvalidKeyException;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.test.GraphTestBase;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.*;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.vertexium.accumulo.ElementMutationBuilder.EMPTY_TEXT;
import static org.vertexium.accumulo.iterator.model.KeyBase.VALUE_SEPARATOR;
import static org.vertexium.util.IterableUtils.toList;

public abstract class AccumuloGraphTestBase extends GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraphTestBase.class);

    @Before
    @Override
    public void before() throws Exception {
        getAccumuloResource().dropGraph();
        super.before();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Graph createGraph() throws VertexiumException {
        return AccumuloGraph.create(new AccumuloGraphConfiguration(getAccumuloResource().createConfig()));
    }

    public abstract AccumuloResource getAccumuloResource();

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        getGraph().createAuthorizations(auths);
        return new AccumuloAuthorizations(auths);
    }

    @Override
    protected Authorizations createButDontAddAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    @Override
    protected boolean isDefaultSearchIndex() {
        return true;
    }

    @Override
    protected boolean isFetchHintNoneVertexQuerySupported() {
        return false;
    }

    @Override
    protected boolean isAdvancedGeoQuerySupported() {
        return false;
    }

    @Test
    public void testDefinePropertiesMultipleGraphs() {
        Graph graph1 = graph;
        Graph graph2 = AccumuloGraph.create(new AccumuloGraphConfiguration(getAccumuloResource().createConfig()));

        graph1.defineProperty("p1").dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        StopWatch timeout = new StopWatch();
        timeout.start();
        while (timeout.getTime() < 5000) {
            assertNotNull("Property definition cache shouldn't clear", graph1.getPropertyDefinition("p1"));

            PropertyDefinition def = graph2.getPropertyDefinition("p1");
            if (def != null) {
                LOGGER.debug("Propagation to graph #2 took %d ms", timeout.getTime());
                break;
            }
        }

        assertNotNull("Property definition didn't propagate to graph #2", graph2.getPropertyDefinition("p1"));
        assertTrue(graph1.getPropertyDefinition("p1").isSortable());
        assertTrue(graph2.getPropertyDefinition("p1").isSortable());

        graph2.defineProperty("p1").dataType(String.class).sortable(false).textIndexHint(TextIndexHint.ALL).define();
        assertFalse(graph2.getPropertyDefinition("p1").isSortable());

        timeout.reset();
        timeout.start();
        while (timeout.getTime() < 5000) {
            PropertyDefinition def = graph1.getPropertyDefinition("p1");
            if (def != null && !def.isSortable()) {
                LOGGER.debug("Propagation to graph #1 took %d ms", timeout.getTime());
                return;
            }
        }
        throw new RuntimeException("Timeout waiting for sortable update to propagate");
    }


    @Test
    public void testStoringEmptyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        Metadata metadata = Metadata.create();
        v1.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        metadata = Metadata.create();
        metadata.add("meta1", "metavalue1", VISIBILITY_EMPTY);
        v2.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().entrySet().size());

        v2 = graph.getVertex("v2", FetchHints.ALL, AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.entrySet().size());
        assertEquals("metavalue1", metadata.getEntry("meta1", VISIBILITY_EMPTY).getValue());

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        ScannerBase vertexScanner = accumuloGraph.createVertexScanner(
            graph.getDefaultFetchHints(),
            AccumuloGraph.SINGLE_VERSION,
            null,
            null,
            new Range("V", "W"),
            AUTHORIZATIONS_EMPTY.getUser()
        );
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

        Metadata metadata = Metadata.create();
        metadata.add("key1_prop2_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop2_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "author", "value_key1_author", metadata, 400L, VISIBILITY_A_AND_B);

        metadata = Metadata.create();
        metadata.add("key1_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "prop1", "value_key1_prop1", metadata, 200L, VISIBILITY_A);

        metadata = Metadata.create();
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
        assertEquals(new Key(new Text("v1"), AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, new Text("a"), 100L), pair.getKey());
        assertEquals(ElementMutationBuilder.EMPTY_VALUE, pair.getValue());
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testGetKeyValuePairsForEdgeMutation() {
        EdgeBuilderByVertexId m = graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, 100L, VISIBILITY_A);

        Metadata metadata = Metadata.create();
        metadata.add("key1_prop2_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop2_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "author", "value_key1_author", metadata, 400L, VISIBILITY_A_AND_B);

        metadata = Metadata.create();
        metadata.add("key1_prop1_m1", "m1_value", VISIBILITY_A);
        metadata.add("key1_prop1_m2", "m2_value", VISIBILITY_A);
        m.addPropertyValue("key1", "prop1", "value_key1_prop1", metadata, 200L, VISIBILITY_A);

        metadata = Metadata.create();
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
        assertEquals(new Key(new Text("e1"), AccumuloEdge.CF_SIGNAL, new Text(LABEL_LABEL1), new Text("a"), 100L), pair.getKey());
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
        org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = new EdgeInfo(getGraph().getNameSubstitutionStrategy().deflate(LABEL_LABEL1), "v2", new Text("a"));
        assertEquals(new Key(new Text("v1"), AccumuloVertex.CF_OUT_EDGE, new Text("e1"), new Text("a"), 100L), pair.getKey());
        assertEquals(edgeInfo.toValue(), pair.getValue());

        pair = keyValuePairs.get(i++);
        edgeInfo = new EdgeInfo(getGraph().getNameSubstitutionStrategy().deflate(LABEL_LABEL1), "v1", new Text("a"));
        assertEquals(new Key(new Text("v2"), AccumuloVertex.CF_IN_EDGE, new Text("e1"), new Text("a"), 100L), pair.getKey());
        assertEquals(edgeInfo.toValue(), pair.getValue());
    }

    protected abstract String substitutionDeflate(String str);

    @Test
    public void testPropertyWithValueSeparator() {
        try {
            graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("prop1" + VALUE_SEPARATOR, "name1", "test", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
            throw new RuntimeException("Should have thrown a bad character exception");
        } catch (VertexiumInvalidKeyException ex) {
            // ok
        }
    }

    @Test
    public void testListSplits() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        SortedSet<Text> keys = new TreeSet<>();
        keys.add(new Text("j"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getVerticesTableName(), keys);

        keys = new TreeSet<>();
        keys.add(new Text("k"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getEdgesTableName(), keys);

        keys = new TreeSet<>();
        keys.add(new Text("l"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getDataTableName(), keys);

        List<org.vertexium.Range> verticesTableSplits = toList(getGraph().listVerticesTableSplits());
        assertEquals(2, verticesTableSplits.size());
        assertEquals(null, verticesTableSplits.get(0).getInclusiveStart());
        assertEquals("j", verticesTableSplits.get(0).getExclusiveEnd());
        assertEquals("j", verticesTableSplits.get(1).getInclusiveStart());
        assertEquals(null, verticesTableSplits.get(1).getExclusiveEnd());

        List<org.vertexium.Range> edgesTableSplits = toList(getGraph().listEdgesTableSplits());
        assertEquals(2, edgesTableSplits.size());
        assertEquals(null, edgesTableSplits.get(0).getInclusiveStart());
        assertEquals("k", edgesTableSplits.get(0).getExclusiveEnd());
        assertEquals("k", edgesTableSplits.get(1).getInclusiveStart());
        assertEquals(null, edgesTableSplits.get(1).getExclusiveEnd());

        List<org.vertexium.Range> dataTableSplits = toList(getGraph().listDataTableSplits());
        assertEquals(2, dataTableSplits.size());
        assertEquals(null, dataTableSplits.get(0).getInclusiveStart());
        assertEquals("l", dataTableSplits.get(0).getExclusiveEnd());
        assertEquals("l", dataTableSplits.get(1).getInclusiveStart());
        assertEquals(null, dataTableSplits.get(1).getExclusiveEnd());
    }

    @Test
    public void testLegacyStreamingPropertyValuesWithTimestampInRowKey() throws Exception {
        String vertexId = "v1";
        graph.prepareVertex(vertexId, VISIBILITY_EMPTY)
            .save(AUTHORIZATIONS_EMPTY);
        graph.flush();

        long timestamp = new Date().getTime();

        String propertyKey = "k1";
        String propertyName = "prop1";
        String propertyValue = "Hello";

        addLegacySPVData(vertexId, timestamp, propertyKey, propertyName, propertyValue);

        getGraph().flush();

        // verify we can still retrieve it
        Vertex v1 = graph.getVertex(vertexId, AUTHORIZATIONS_EMPTY);
        StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue(propertyKey, propertyName);
        assertNotNull("spv should not be null", spv);
        assertEquals(propertyValue, IOUtils.toString(spv.getInputStream()));
    }

    // need to add it manually because the key format changed
    private void addLegacySPVData(
        String vertexId,
        long timestamp,
        String propertyKey,
        String propertyName,
        String propertyValue
    ) throws MutationsRejectedException {
        String dataRowKey = new DataTableRowKey(vertexId, propertyKey, propertyName).getRowKey() + VALUE_SEPARATOR + timestamp;

        Mutation addPropertyMutation = new Mutation(vertexId);
        byte[] data = propertyValue.getBytes();
        StreamingPropertyValue spv = StreamingPropertyValue.create(propertyValue);
        StreamingPropertyValueTableRef spvValue = new StreamingPropertyValueTableRef(dataRowKey, spv, data);
        Metadata metadata = Metadata.create();
        Property property = new MutablePropertyImpl(propertyKey, propertyName, spvValue, metadata, timestamp, new HashSet<>(), new Visibility(""), FetchHints.ALL);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getGraph().getNameSubstitutionStrategy());
        addPropertyMutation.put(AccumuloElement.CF_PROPERTY, columnQualifier, new Value(getGraph().getVertexiumSerializer().objectToBytes(spvValue)));
        getGraph().getVerticesWriter().addMutation(addPropertyMutation);

        Mutation addDataMutation = new Mutation(dataRowKey);
        addDataMutation.put(EMPTY_TEXT, EMPTY_TEXT, timestamp - 1000, new Value(data));
        getGraph().getDataWriter().addMutation(addDataMutation);

        getGraph().flush();
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }
}
