package org.vertexium.tools;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.*;
import org.vertexium.id.UUIDIdGenerator;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.search.DefaultSearchIndex;
import org.vertexium.test.GraphTestBase;
import org.vertexium.test.util.LargeStringInputStream;
import org.vertexium.util.IterableUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class BackupRestoreTest {
    public final Authorizations AUTHORIZATIONS_A;
    public final Authorizations AUTHORIZATIONS_B;
    public final Authorizations AUTHORIZATIONS_C;
    public final Authorizations AUTHORIZATIONS_A_AND_B;

    public BackupRestoreTest() {
        AUTHORIZATIONS_A = createAuthorizations("a");
        AUTHORIZATIONS_B = createAuthorizations("b");
        AUTHORIZATIONS_C = createAuthorizations("c");
        AUTHORIZATIONS_A_AND_B = createAuthorizations("a", "b");
    }

    private Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    protected Graph createGraph() {
        Map config = new HashMap();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Test
    public void testSaveAndLoad() throws IOException, ClassNotFoundException {
        Graph graph = createGraph();

        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", GraphTestBase.VISIBILITY_A);

        int largePropertyValueSize = 1000;
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(largePropertyValueSize));
        StreamingPropertyValue largeDataValue = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);

        Vertex v1 = graph.prepareVertex("v1", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1a", "prop1", "value1a", prop1Metadata, GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1b", "prop1", "value1b", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id2", "prop2", "value2", GraphTestBase.VISIBILITY_B)
                .setProperty("largeData", largeDataValue, GraphTestBase.VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", GraphTestBase.VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", GraphTestBase.VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge("e1to2", v1, v2, "label1", GraphTestBase.VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to3", v1, v3, "label1", GraphTestBase.VISIBILITY_B, AUTHORIZATIONS_B);

        File tmp = File.createTempFile(getClass().getName(), ".json");
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            System.out.println("saving graph to: " + tmp);
            GraphBackup graphBackup = new GraphBackup();
            graphBackup.save(graph, out, AUTHORIZATIONS_A_AND_B);
        }

        try (FileInputStream in = new FileInputStream(tmp)) {
            Graph loadedGraph = createGraph();
            GraphRestore graphRestore = new GraphRestore();
            graphRestore.restore(loadedGraph, in, AUTHORIZATIONS_A_AND_B);

            Assert.assertEquals(3, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_A_AND_B)));
            Assert.assertEquals(2, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_A)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getVertices(AUTHORIZATIONS_B)));
            Assert.assertEquals(2, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_A_AND_B)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_A)));
            Assert.assertEquals(1, IterableUtils.count(loadedGraph.getEdges(AUTHORIZATIONS_B)));

            v1 = loadedGraph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(2, IterableUtils.count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
            Iterable<Property> properties = v1.getProperties();
            boolean prop1_id1a_found = false;
            boolean prop1_id1b_found = false;
            for (Property property : properties) {
                if (property.getName().equals("prop1")) {
                    if (property.getKey().equals("id1a")) {
                        prop1_id1a_found = true;
                        assertEquals("value1a", property.getValue());
                    }
                    if (property.getKey().equals("id1b")) {
                        prop1_id1b_found = true;
                        assertEquals("value1b", property.getValue());
                    }
                }
            }
            assertTrue("prop1[id1a] not found", prop1_id1a_found);
            assertTrue("prop1[id1b] not found", prop1_id1b_found);
            assertEquals("value2", v1.getPropertyValue("prop2", 0));
            StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue("largeData", 0);
            assertNotNull("largeData property not found", spv);
            assertEquals(String.class, spv.getValueType());
            assertEquals(expectedLargeValue, IOUtils.toString(spv.getInputStream()));

            v2 = loadedGraph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(1, IterableUtils.count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

            v3 = loadedGraph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
            Assert.assertEquals(1, IterableUtils.count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        }

        tmp.delete();
    }
}
