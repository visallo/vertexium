package org.vertexium.test;


import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.*;
import org.vertexium.event.*;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.*;
import org.vertexium.search.DefaultSearchIndex;
import org.vertexium.search.IndexHint;
import org.vertexium.search.SearchIndex;
import org.vertexium.test.util.LargeStringInputStream;
import org.vertexium.type.*;
import org.vertexium.util.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;
import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;

@RunWith(JUnit4.class)
public abstract class GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphTestBase.class);
    public static final String VISIBILITY_A_STRING = "a";
    public static final String VISIBILITY_B_STRING = "b";
    public static final String VISIBILITY_C_STRING = "c";
    public static final String VISIBILITY_MIXED_CASE_STRING = "MIXED_CASE_a";
    public static final Visibility VISIBILITY_A = new Visibility(VISIBILITY_A_STRING);
    public static final Visibility VISIBILITY_A_AND_B = new Visibility("a&b");
    public static final Visibility VISIBILITY_B = new Visibility("b");
    public static final Visibility VISIBILITY_MIXED_CASE_a = new Visibility("((MIXED_CASE_a))|b");
    public static final Visibility VISIBILITY_EMPTY = new Visibility("");
    public final Authorizations AUTHORIZATIONS_A;
    public final Authorizations AUTHORIZATIONS_B;
    public final Authorizations AUTHORIZATIONS_C;
    public final Authorizations AUTHORIZATIONS_MIXED_CASE_a_AND_B;
    public final Authorizations AUTHORIZATIONS_A_AND_B;
    public final Authorizations AUTHORIZATIONS_EMPTY;
    public final Authorizations AUTHORIZATIONS_BAD;
    public final Authorizations AUTHORIZATIONS_ALL;
    public static final int LARGE_PROPERTY_VALUE_SIZE = 1024 * 1024 + 1;

    protected Graph graph;
    protected List<GraphEvent> graphEvents;

    protected abstract Graph createGraph() throws Exception;

    public Graph getGraph() {
        return graph;
    }

    public GraphTestBase() {
        AUTHORIZATIONS_A = createAuthorizations("a");
        AUTHORIZATIONS_B = createAuthorizations("b");
        AUTHORIZATIONS_C = createAuthorizations("c");
        AUTHORIZATIONS_A_AND_B = createAuthorizations("a", "b");
        AUTHORIZATIONS_MIXED_CASE_a_AND_B = createAuthorizations("MIXED_CASE_a", "b");
        AUTHORIZATIONS_EMPTY = createAuthorizations();
        AUTHORIZATIONS_BAD = createAuthorizations("bad");
        AUTHORIZATIONS_ALL = createAuthorizations("a", "b", "c", "MIXED_CASE_a");
    }

    protected abstract Authorizations createAuthorizations(String... auths);

    @Before
    public void before() throws Exception {
        graph = createGraph();
        graphEvents = new ArrayList<>();
        graph.addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                graphEvents.add(graphEvent);
            }
        });
    }

    @After
    public void after() throws Exception {
        if (graph != null) {
            graph.shutdown();
            graph = null;
        }
    }

    @Test
    public void testAddVertexWithId() {
        Vertex vertexAdded = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        assertNotNull(vertexAdded);
        assertEquals("v1", vertexAdded.getId());
        graph.flush();

        Vertex v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_A, v.getVisibility());

        v = graph.getVertex("", AUTHORIZATIONS_A);
        assertNull(v);

        v = graph.getVertex(null, AUTHORIZATIONS_A);
        assertNull(v);

        assertEvents(
                new AddVertexEvent(graph, vertexAdded)
        );
    }

    @Test
    public void testAddVertexWithoutId() {
        Vertex vertexAdded = graph.addVertex(VISIBILITY_A, AUTHORIZATIONS_A);
        assertNotNull(vertexAdded);
        String vertexId = vertexAdded.getId();
        assertNotNull(vertexId);
        graph.flush();

        Vertex v = graph.getVertex(vertexId, AUTHORIZATIONS_A);
        assertNotNull(v);
        assertNotNull(vertexId);

        assertEvents(
                new AddVertexEvent(graph, vertexAdded)
        );
    }

    @Test
    public void testGetSingleVertexWithSameRowPrefix() {
        graph.addVertex("prefix", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        graph.addVertex("prefixA", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        graph.flush();

        Vertex v = graph.getVertex("prefix", AUTHORIZATIONS_EMPTY);
        assertEquals("prefix", v.getId());

        v = graph.getVertex("prefixA", AUTHORIZATIONS_EMPTY);
        assertEquals("prefixA", v.getId());
    }

    @Test
    public void testStreamingPropertyValueReadAsString() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("spv", StreamingPropertyValue.create("Hello World"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals("Hello World", ((StreamingPropertyValue) v1.getPropertyValue("spv")).readToString());
        assertEquals("Wor", ((StreamingPropertyValue) v1.getPropertyValue("spv")).readToString(6, 3));
        assertEquals("", ((StreamingPropertyValue) v1.getPropertyValue("spv")).readToString("Hello World".length(), 1));
        assertEquals("Hello World", ((StreamingPropertyValue) v1.getPropertyValue("spv")).readToString(0, 100));
    }

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    @Test
    public void testAddStreamingPropertyValue() throws IOException, InterruptedException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class, 6);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()),
                String.class, expectedLargeValue.length()
        );
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        Iterable<Object> propSmallValues = v1.getPropertyValues("propSmall");
        Assert.assertEquals(1, count(propSmallValues));
        Object propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        StreamingPropertyValue value = (StreamingPropertyValue) propSmallValue;
        assertEquals(String.class, value.getValueType());
        assertEquals("value1".getBytes().length, value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        Iterable<Object> propLargeValues = v1.getPropertyValues(largePropertyName);
        Assert.assertEquals(1, count(propLargeValues));
        Object propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        propSmallValues = v1.getPropertyValues("propSmall");
        Assert.assertEquals(1, count(propSmallValues));
        propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propSmallValue;
        assertEquals(String.class, value.getValueType());
        assertEquals("value1".getBytes().length, value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        propLargeValues = v1.getPropertyValues(largePropertyName);
        Assert.assertEquals(1, count(propLargeValues));
        propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
    }

    @Test
    public void testAddVertexPropertyWithMetadata() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v = graph.getVertex("v1", AUTHORIZATIONS_A);
        if (v instanceof HasTimestamp) {
            assertTrue("timestamp should be more than 0", v.getTimestamp() > 0);
        }

        Assert.assertEquals(1, count(v.getProperties("prop1")));
        Property prop1 = v.getProperties("prop1").iterator().next();
        if (prop1 instanceof HasTimestamp) {
            assertTrue("timestamp should be more than 0", prop1.getTimestamp() > 0);
        }

        prop1Metadata = prop1.getMetadata();
        assertNotNull(prop1Metadata);
        assertEquals(1, prop1Metadata.entrySet().size());
        assertEquals("metadata1Value", prop1Metadata.getEntry("metadata1", VISIBILITY_A).getValue());

        prop1Metadata.add("metadata2", "metadata2Value", VISIBILITY_A);
        v.prepareMutation()
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertEquals(2, prop1Metadata.entrySet().size());
        assertEquals("metadata1Value", prop1Metadata.getEntry("metadata1", VISIBILITY_A).getValue());
        assertEquals("metadata2Value", prop1Metadata.getEntry("metadata2", VISIBILITY_A).getValue());

        // make sure that when we update the value the metadata is not carried over
        prop1Metadata = new Metadata();
        v.setProperty("prop1", "value2", prop1Metadata, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        assertEquals("value2", prop1.getValue());
        prop1Metadata = prop1.getMetadata();
        assertEquals(0, prop1Metadata.entrySet().size());
    }

    @Test
    public void testAddVertexWithProperties() {
        Vertex vertexAdded = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(1, count(vertexAdded.getProperties("prop1")));
        assertEquals("value1", vertexAdded.getPropertyValues("prop1").iterator().next());
        Assert.assertEquals(1, count(vertexAdded.getProperties("prop2")));
        assertEquals("value2", vertexAdded.getPropertyValues("prop2").iterator().next());
        graph.flush();

        Vertex v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1", v.getPropertyValues("prop1").iterator().next());
        Assert.assertEquals(1, count(v.getProperties("prop2")));
        assertEquals("value2", v.getPropertyValues("prop2").iterator().next());

        assertEvents(
                new AddVertexEvent(graph, vertexAdded),
                new AddPropertyEvent(graph, vertexAdded, vertexAdded.getProperty("prop1")),
                new AddPropertyEvent(graph, vertexAdded, vertexAdded.getProperty("prop2"))
        );
        graphEvents.clear();

        v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v.prepareMutation()
                .addPropertyValue("key1", "prop1Mutation", "value1Mutation", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(1, count(v.getProperties("prop1Mutation")));
        assertEquals("value1Mutation", v.getPropertyValues("prop1Mutation").iterator().next());
        assertEvents(
                new AddPropertyEvent(graph, vertexAdded, vertexAdded.getProperty("prop1Mutation"))
        );
    }

    @Test
    public void testNullPropertyValue() {
        try {
            graph.prepareVertex("v1", VISIBILITY_EMPTY)
                    .setProperty("prop1", null, VISIBILITY_A)
                    .save(AUTHORIZATIONS_A_AND_B);
            throw new VertexiumException("expected null check");
        } catch (NullPointerException ex) {
            assertTrue(ex.getMessage().contains("prop1"));
        }
    }

    @Test
    public void testConcurrentModificationOfProperties() {
        Vertex v = graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        int i = 0;
        for (Property p : v.getProperties()) {
            assertNotNull(p.toString());
            if (i == 0) {
                v.setProperty("prop3", "value3", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
            }
            i++;
        }
    }

    @Test
    public void testAddVertexWithPropertiesWithTwoDifferentVisibilities() {
        Vertex v = graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("prop1", "value1a", VISIBILITY_A)
                .setProperty("prop1", "value1b", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(v.getProperties("prop1")));
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(v.getProperties("prop1")));

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1a", v.getPropertyValue("prop1"));

        v = graph.getVertex("v1", AUTHORIZATIONS_B);
        Assert.assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1b", v.getPropertyValue("prop1"));
    }

    @Test
    public void testMultivaluedProperties() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1a", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2a", VISIBILITY_A)
                .addPropertyValue("propid3a", "prop3", "value3a", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value1a", v.getPropertyValues("prop1").iterator().next());
        assertEquals("value2a", v.getPropertyValues("prop2").iterator().next());
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        Assert.assertEquals(3, count(v.getProperties()));

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1b", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2b", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getPropertyValues("prop1")));
        assertEquals("value1b", v.getPropertyValues("prop1").iterator().next());
        Assert.assertEquals(1, count(v.getPropertyValues("prop2")));
        assertEquals("value2b", v.getPropertyValues("prop2").iterator().next());
        Assert.assertEquals(1, count(v.getPropertyValues("prop3")));
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        Assert.assertEquals(3, count(v.getProperties()));

        v.addPropertyValue("propid1b", "prop1", "value1a-new", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        org.vertexium.test.util.IterableUtils.assertContains("value1b", v.getPropertyValues("prop1"));
        org.vertexium.test.util.IterableUtils.assertContains("value1a-new", v.getPropertyValues("prop1"));
        Assert.assertEquals(4, count(v.getProperties()));
    }

    @Test
    public void testMultivaluedPropertyOrder() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("a", "prop", "a", VISIBILITY_A)
                .addPropertyValue("aa", "prop", "aa", VISIBILITY_A)
                .addPropertyValue("b", "prop", "b", VISIBILITY_A)
                .addPropertyValue("0", "prop", "0", VISIBILITY_A)
                .addPropertyValue("A", "prop", "A", VISIBILITY_A)
                .addPropertyValue("Z", "prop", "Z", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("0", v1.getPropertyValue("prop", 0));
        assertEquals("A", v1.getPropertyValue("prop", 1));
        assertEquals("Z", v1.getPropertyValue("prop", 2));
        assertEquals("a", v1.getPropertyValue("prop", 3));
        assertEquals("aa", v1.getPropertyValue("prop", 4));
        assertEquals("b", v1.getPropertyValue("prop", 5));
    }

    @Test
    public void testDeleteProperty() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1a", VISIBILITY_A)
                .addPropertyValue("propid1b", "prop1", "value1b", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2a", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();
        this.graphEvents.clear();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Property prop1_propid1a = v.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v.getProperty("propid1b", "prop1");
        v.deleteProperties("prop1", AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(v.getProperties()));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getProperties()));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop2", "value2a").vertices()));
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1a").vertices()));
        assertEvents(
                new DeletePropertyEvent(graph, v, prop1_propid1a),
                new DeletePropertyEvent(graph, v, prop1_propid1b)
        );
        this.graphEvents.clear();

        Property prop2_propid2a = v.getProperty("propid2a", "prop2");
        v.deleteProperty("propid2a", "prop2", AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(0, count(v.getProperties()));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v.getProperties()));

        assertEvents(
                new DeletePropertyEvent(graph, v, prop2_propid2a)
        );
    }

    @Test
    public void testDeletePropertyWithMutation() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("propid1a", "prop1", "value1a", VISIBILITY_A)
                .addPropertyValue("propid1b", "prop1", "value1b", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2a", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "edge1", VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        this.graphEvents.clear();

        // delete multiple properties
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Property prop1_propid1a = v1.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v1.getProperty("propid1b", "prop1");
        v1.prepareMutation()
                .deleteProperties("prop1")
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(v1.getProperties()));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getProperties()));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop2", "value2a").vertices()));
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1a").vertices()));
        assertEvents(
                new DeletePropertyEvent(graph, v1, prop1_propid1a),
                new DeletePropertyEvent(graph, v1, prop1_propid1b)
        );
        this.graphEvents.clear();

        // delete property with key and name
        Property prop2_propid2a = v1.getProperty("propid2a", "prop2");
        v1.prepareMutation()
                .deleteProperties("propid2a", "prop2")
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(0, count(v1.getProperties()));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v1.getProperties()));
        assertEvents(
                new DeletePropertyEvent(graph, v1, prop2_propid2a)
        );
        this.graphEvents.clear();

        // delete property from edge
        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        Property edgeProperty = e1.getProperty("key1", "prop1");
        e1.prepareMutation()
                .deleteProperties("key1", "prop1")
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(0, count(e1.getProperties()));
        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(e1.getProperties()));
        assertEvents(
                new DeletePropertyEvent(graph, e1, edgeProperty)
        );
    }

    @Test
    public void testDeleteElement() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);

        v.prepareMutation()
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1").vertices()));

        graph.deleteVertex(v.getId(), AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v);
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1").vertices()));
    }

    @Test
    public void testDeleteVertex() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.deleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A)));
    }

    @Test
    public void testSoftDeleteVertex() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", "label1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        Vertex v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, v2.getEdgeCount(Direction.BOTH, AUTHORIZATIONS_A));

        graph.softDeleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(0, v2.getEdgeCount(Direction.BOTH, AUTHORIZATIONS_A));

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v4", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(4, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertResultsCount(3, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());

        graph.softDeleteVertex("v3", AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(3, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertResultsCount(2, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());
    }

    @Test
    public void testGetSoftDeletedElementWithFetchHintsAndTimestamp() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        long beforeDeleteTime = IncreasingTime.currentTimeMillis();
        graph.softDeleteEdge(e1, AUTHORIZATIONS_A);
        graph.softDeleteVertex(v1, AUTHORIZATIONS_A);
        graph.flush();

        assertNull(graph.getEdge(e1.getId(), AUTHORIZATIONS_A));
        assertNull(graph.getEdge(e1.getId(), FetchHint.ALL, AUTHORIZATIONS_A));
        assertNull(graph.getEdge(e1.getId(), FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), FetchHint.ALL, AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));

        assertNotNull(graph.getEdge(e1.getId(), FetchHint.ALL, beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getEdge(e1.getId(), FetchHint.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getVertex(v1.getId(), FetchHint.ALL, beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getVertex(v1.getId(), FetchHint.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
    }

    @Test
    public void testSoftDeleteEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("e1", v1, v2, "label1", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("e2", v1, v3, "label1", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        graph.softDeleteEdge(e1, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = graph.getVertex("v2", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = graph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v3.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = graph.getVertex("v3", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v3.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testSoftDeleteProperty() throws InterruptedException {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());

        graph.getVertex("v1", AUTHORIZATIONS_A).softDeleteProperties("name1", AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());

        graph.getVertex("v1", AUTHORIZATIONS_A).softDeleteProperties("name1", AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());
    }

    @Test
    public void testSoftDeletePropertyThroughMutation() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .softDeleteProperties("name1")
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(1, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .softDeleteProperties("name1")
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));
        assertResultsCount(0, graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices());
    }

    @Test
    public void testSoftDeletePropertyOnEdgeNotIndexed() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        ElementBuilder<Edge> elementBuilder = graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_B)
                .setProperty("prop1", "value1", VISIBILITY_B);
        elementBuilder.setIndexHint(IndexHint.DO_NOT_INDEX);
        Edge e1 = elementBuilder.save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ExistingElementMutation<Edge> m = e1.prepareMutation();
        m.softDeleteProperty("prop1", VISIBILITY_B);
        m.setIndexHint(IndexHint.DO_NOT_INDEX);
        m.save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, IterableUtils.count(e1.getProperties()));
    }

    @Test
    public void testSoftDeletePropertyWithVisibility() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("value1", v1.getPropertyValues("name1"));
        org.vertexium.test.util.IterableUtils.assertContains("value2", v1.getPropertyValues("name1"));

        graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).softDeleteProperty("key1", "name1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getPropertyValues("key1", "name1")));
        org.vertexium.test.util.IterableUtils.assertContains("value2", graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getPropertyValues("name1"));
    }

    @Test
    public void testSoftDeletePropertyThroughMutationWithVisibility() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_A)
                .addPropertyValue("key1", "name1", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("value1", v1.getPropertyValues("name1"));
        org.vertexium.test.util.IterableUtils.assertContains("value2", v1.getPropertyValues("name1"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B)
                .prepareMutation()
                .softDeleteProperty("key1", "name1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(v1.getProperties()));
        assertEquals(1, count(v1.getPropertyValues("key1", "name1")));
        org.vertexium.test.util.IterableUtils.assertContains("value2", v1.getPropertyValues("name1"));
    }

    @Test
    public void testSoftDeletePropertyOnAHiddenVertex() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("key1", "name1", "value1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        graph.markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        v1.softDeleteProperty("key1", "name1", AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        assertNull(v1.getProperty("key1", "name1", VISIBILITY_EMPTY));
    }

    @Test
    public void testMarkHiddenWithVisibilityChange() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "firstName", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(v1.getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("Joe", v1.getPropertyValues("firstName"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", "Joseph", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        List<Property> properties = IterableUtils.toList(v1.getProperties());
        assertEquals(2, count(properties));

        boolean foundJoeProp = false;
        boolean foundJosephProp = false;
        for (Property property : properties) {
            if (property.getName().equals("firstName")) {
                if (property.getKey().equals("key1") && property.getValue().equals("Joe")) {
                    foundJoeProp = true;
                    assertTrue("should be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A));
                } else if (property.getKey().equals("key1") && property.getValue().equals("Joseph")) {
                    if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundJosephProp = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Joseph property value not found", foundJosephProp);
        assertTrue("Joe property value not found", foundJoeProp);
    }

    @Test
    public void testSoftDeleteWithVisibilityChanges() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "firstName", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(v1.getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("Joe", v1.getPropertyValues("firstName"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", "Joseph", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v1.softDeleteProperty("key1", "firstName", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v1.markPropertyVisible("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v1.addPropertyValue("key1", "firstName", "Joseph", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Property> properties = IterableUtils.toList(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        Property property = properties.iterator().next();
        assertEquals(VISIBILITY_A, property.getVisibility());
        assertEquals("Joseph", property.getValue());

        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A)
                .addPropertyValue("key1", "firstName", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(v2.getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("Joe", v2.getPropertyValues("firstName"));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
        v2.markPropertyHidden("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v2.addPropertyValue("key1", "firstName", "Joseph", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v2.softDeleteProperty("key1", "firstName", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();
        v2.markPropertyVisible("key1", "firstName", VISIBILITY_A, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        v2.addPropertyValue("key1", "firstName", "Joe", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        properties = IterableUtils.toList(graph.getVertex("v2", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        property = properties.iterator().next();
        assertEquals(VISIBILITY_A, property.getVisibility());
        assertEquals("Joe", property.getValue());
    }

    @Test
    public void testMarkPropertyVisible() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "firstName", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals(1, count(v1.getProperties()));
        org.vertexium.test.util.IterableUtils.assertContains("Joe", v1.getPropertyValues("firstName"));

        long t = IncreasingTime.currentTimeMillis();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.markPropertyHidden("key1", "firstName", VISIBILITY_A, t, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        t += 10;
        List<Property> properties = IterableUtils.toList(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));

        long beforeMarkPropertyVisibleTimestamp = t;
        t += 10;

        v1.markPropertyVisible("key1", "firstName", VISIBILITY_A, t, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        t += 10;
        properties = IterableUtils.toList(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.ALL, beforeMarkPropertyVisibleTimestamp, AUTHORIZATIONS_A_AND_B);
        assertNotNull("could not find v1 before timestamp " + beforeMarkPropertyVisibleTimestamp + " current time " + t, v1);
        properties = IterableUtils.toList(v1.getProperties());
        assertEquals(0, count(properties));
    }

    @Test
    public void testAddVertexWithVisibility() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_ALL);
        graph.flush();

        Iterable<Vertex> cVertices = graph.getVertices(AUTHORIZATIONS_C);
        Assert.assertEquals(0, count(cVertices));

        Iterable<Vertex> aVertices = graph.getVertices(AUTHORIZATIONS_A);
        assertEquals("v1", IterableUtils.single(aVertices).getId());

        Iterable<Vertex> bVertices = graph.getVertices(AUTHORIZATIONS_B);
        assertEquals("v2", IterableUtils.single(bVertices).getId());

        Iterable<Vertex> allVertices = graph.getVertices(AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(allVertices));
    }

    @Test
    public void testAddMultipleVertices() {
        List<ElementBuilder<Vertex>> elements = new ArrayList<>();
        elements.add(graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "v1", VISIBILITY_A));
        elements.add(graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("prop1", "v2", VISIBILITY_A));
        Iterable<Vertex> vertices = graph.addVertices(elements, AUTHORIZATIONS_A_AND_B);
        assertVertexIds(vertices, new String[]{"v1", "v2"});
        graph.flush();

        if (graph instanceof GraphWithSearchIndex) {
            ((GraphWithSearchIndex) graph).getSearchIndex().addElements(graph, vertices, AUTHORIZATIONS_A_AND_B);
            assertVertexIds(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "v1").vertices(), new String[]{"v1"});
            assertVertexIds(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "v2").vertices(), new String[]{"v2"});
        }
    }

    @Test
    public void testGetVerticesWithIds() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v1b", VISIBILITY_A)
                .setProperty("prop1", "v1b", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("prop1", "v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("prop1", "v3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<String> ids = new ArrayList<>();
        ids.add("v2");
        ids.add("v1");

        Iterable<Vertex> vertices = graph.getVertices(ids, AUTHORIZATIONS_A);
        boolean foundV1 = false, foundV2 = false;
        for (Vertex v : vertices) {
            if (v.getId().equals("v1")) {
                assertEquals("v1", v.getPropertyValue("prop1"));
                foundV1 = true;
            } else if (v.getId().equals("v2")) {
                assertEquals("v2", v.getPropertyValue("prop1"));
                foundV2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + v.getId(), false);
            }
        }
        assertTrue("v1 not found", foundV1);
        assertTrue("v2 not found", foundV2);

        List<Vertex> verticesInOrder = graph.getVerticesInOrder(ids, AUTHORIZATIONS_A);
        assertEquals(2, verticesInOrder.size());
        assertEquals("v2", verticesInOrder.get(0).getId());
        assertEquals("v1", verticesInOrder.get(1).getId());
    }

    @Test
    public void testGetVerticesWithPrefix() {
        graph.addVertex("a", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("aa", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("az", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("b", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.flush();

        List<Vertex> vertices = sortById(toList(graph.getVerticesWithPrefix("a", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, new String[]{"a", "aa", "az"});

        vertices = sortById(toList(graph.getVerticesWithPrefix("b", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, new String[]{"b"});

        vertices = sortById(toList(graph.getVerticesWithPrefix("c", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, new String[]{});
    }

    @Test
    public void testGetVerticesInRange() {
        graph.addVertex("a", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("aa", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("az", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("b", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.flush();

        List<Vertex> vertices = toList(graph.getVerticesInRange(new Range(null, "a"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, new String[]{});

        vertices = toList(graph.getVerticesInRange(new Range(null, "b"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, new String[]{"a", "aa", "az"});

        vertices = toList(graph.getVerticesInRange(new Range(null, "bb"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, new String[]{"a", "aa", "az", "b"});

        vertices = toList(graph.getVerticesInRange(new Range(null, null), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, new String[]{"a", "aa", "az", "b"});
    }

    @Test
    public void testGetEdgesInRange() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("a", "v1", "v2", "label1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("aa", "v1", "v2", "label1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("az", "v1", "v2", "label1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("b", "v1", "v2", "label1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.flush();

        List<Edge> edges = toList(graph.getEdgesInRange(new Range(null, "a"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, new String[]{});

        edges = toList(graph.getEdgesInRange(new Range(null, "b"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, new String[]{"a", "aa", "az"});

        edges = toList(graph.getEdgesInRange(new Range(null, "bb"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, new String[]{"a", "aa", "az", "b"});

        edges = toList(graph.getEdgesInRange(new Range(null, null), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, new String[]{"a", "aa", "az", "b"});
    }

    @Test
    public void testGetEdgesWithIds() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "", VISIBILITY_A)
                .setProperty("prop1", "e1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1a", v1, v2, "", VISIBILITY_A)
                .setProperty("prop1", "e1a", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", v1, v3, "", VISIBILITY_A)
                .setProperty("prop1", "e2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", v2, v3, "", VISIBILITY_A)
                .setProperty("prop1", "e3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<String> ids = new ArrayList<>();
        ids.add("e1");
        ids.add("e2");
        Iterable<Edge> edges = graph.getEdges(ids, AUTHORIZATIONS_A);
        boolean foundE1 = false, foundE2 = false;
        for (Edge e : edges) {
            if (e.getId().equals("e1")) {
                assertEquals("e1", e.getPropertyValue("prop1"));
                foundE1 = true;
            } else if (e.getId().equals("e2")) {
                assertEquals("e2", e.getPropertyValue("prop1"));
                foundE2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + e.getId(), false);
            }
        }
        assertTrue("e1 not found", foundE1);
        assertTrue("e2 not found", foundE2);
    }

    @Test
    public void testMarkVertexAndPropertiesHidden() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "age", 25, VISIBILITY_EMPTY)
                .addPropertyValue("k2", "age", 30, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_ALL);
        graph.markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        for (Property property : v1.getProperties()) {
            v1.markPropertyHidden(property, VISIBILITY_A, AUTHORIZATIONS_ALL);
        }
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull("v1 was found", v1);

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
        assertNotNull("could not find v1", v1);
        assertEquals(2, count(v1.getProperties()));
        assertEquals(25, v1.getPropertyValue("k1", "age"));
        assertEquals(30, v1.getPropertyValue("k2", "age"));

        v1 = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
        graph.markVertexVisible(v1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1AfterVisible = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull("could not find v1", v1AfterVisible);
        assertEquals(0, count(v1AfterVisible.getProperties()));

        for (Property property : v1.getProperties()) {
            v1.markPropertyVisible(property, VISIBILITY_A, AUTHORIZATIONS_ALL);
        }
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull("could not find v1", v1);
        assertEquals(2, count(v1.getProperties()));
        assertEquals(25, v1.getPropertyValue("k1", "age"));
        assertEquals(30, v1.getPropertyValue("k2", "age"));
    }

    @Test
    public void testMarkVertexHidden() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("v1tov2", v1, v2, "test", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        List<String> vertexIdList = new ArrayList<>();
        vertexIdList.add("v1");
        vertexIdList.add("v2");
        vertexIdList.add("bad"); // add "bad" to the end of the list to test ordering of results
        Map<String, Boolean> verticesExist = graph.doVerticesExist(vertexIdList, AUTHORIZATIONS_A);
        assertEquals(3, vertexIdList.size());
        assertTrue("v1 exist", verticesExist.get("v1"));
        assertTrue("v2 exist", verticesExist.get("v2"));
        assertFalse("bad exist", verticesExist.get("bad"));

        assertTrue("v1 exists (auth A)", graph.doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", graph.doesVertexExist("v1", AUTHORIZATIONS_B));
        assertTrue("v1 exists (auth A&B)", graph.doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.markVertexHidden(v1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        graph.flush();

        assertTrue("v1 exists (auth A)", graph.doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", graph.doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", graph.doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_B)));
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        assertFalse("v1 exists (auth A)", graph.doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", graph.doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", graph.doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_B)));
        Assert.assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_A)));
        assertNull("found v1 but shouldn't have", graph.getVertex("v1", FetchHint.ALL, AUTHORIZATIONS_A));
        Vertex v1Hidden = graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        assertNotNull("did not find v1 but should have", v1Hidden);
        assertTrue("v1 should be hidden", v1Hidden.isHidden(AUTHORIZATIONS_A));

        graph.markVertexVisible(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        assertTrue("v1 exists (auth A)", graph.doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", graph.doesVertexExist("v1", AUTHORIZATIONS_B));
        assertFalse("v1 exists (auth A&B)", graph.doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_B)));
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.markVertexVisible(v1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        graph.flush();

        assertTrue("v1 exists (auth A)", graph.doesVertexExist("v1", AUTHORIZATIONS_A));
        assertFalse("v1 exists (auth B)", graph.doesVertexExist("v1", AUTHORIZATIONS_B));
        assertTrue("v1 exists (auth A&B)", graph.doesVertexExist("v1", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));
    }

    @Test
    public void testMarkEdgeHidden() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Edge e1 = graph.addEdge("v1tov2", v1, v2, "test", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("v2tov3", v2, v3, "test", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        List<String> edgeIdList = new ArrayList<>();
        edgeIdList.add("v1tov2");
        edgeIdList.add("v2tov3");
        edgeIdList.add("bad");
        Map<String, Boolean> edgesExist = graph.doEdgesExist(edgeIdList, AUTHORIZATIONS_A);
        assertEquals(3, edgeIdList.size());
        assertTrue("v1tov2 exist", edgesExist.get("v1tov2"));
        assertTrue("v2tov3 exist", edgesExist.get("v2tov3"));
        assertFalse("bad exist", edgesExist.get("bad"));

        assertTrue("v1tov2 exists (auth A)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertTrue("v1tov2 exists (auth A&B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(3, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(graph.getEdges(AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths("v1", "v3", 10, AUTHORIZATIONS_A_AND_B)));

        graph.markEdgeHidden(e1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        graph.flush();

        assertTrue("v1tov2 exists (auth A)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertFalse("v1tov2 exists (auth A&B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(2, count(graph.getEdges(AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_B)));
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.findPaths("v1", "v3", 10, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths("v1", "v3", 10, AUTHORIZATIONS_A)));
        assertNull("found e1 but shouldn't have", graph.getEdge("v1tov2", FetchHint.ALL, AUTHORIZATIONS_A_AND_B));
        Edge e1Hidden = graph.getEdge("v1tov2", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertNotNull("did not find e1 but should have", e1Hidden);
        assertTrue("e1 should be hidden", e1Hidden.isHidden(AUTHORIZATIONS_A_AND_B));

        graph.markEdgeVisible(e1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        graph.flush();

        assertTrue("v1tov2 exists (auth A)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A));
        assertFalse("v1tov2 exists (auth B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_B));
        assertTrue("v1tov2 exists (auth A&B)", graph.doesEdgeExist("v1tov2", AUTHORIZATIONS_A_AND_B));
        Assert.assertEquals(3, count(graph.getVertices(AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(graph.getEdges(AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths("v1", "v3", 10, AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testMarkPropertyHidden() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_B)
                .addPropertyValue("key2", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Assert.assertEquals(3, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties("prop1")));

        v1.markPropertyHidden("key1", "prop1", VISIBILITY_A, VISIBILITY_A_AND_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Property> properties = toList(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties("prop1"));
        Assert.assertEquals(2, count(properties));
        boolean foundProp1Key2 = false;
        boolean foundProp1Key1VisB = false;
        for (Property property : properties) {
            if (property.getName().equals("prop1")) {
                if (property.getKey().equals("key2")) {
                    foundProp1Key2 = true;
                } else if (property.getKey().equals("key1")) {
                    if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundProp1Key1VisB = true;
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Prop1Key2 not found", foundProp1Key2);
        assertTrue("Prop1Key1VisB not found", foundProp1Key1VisB);

        List<Property> hiddenProperties = toList(graph.getVertex("v1", FetchHint.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(3, hiddenProperties.size());
        boolean foundProp1Key1VisA = false;
        foundProp1Key2 = false;
        foundProp1Key1VisB = false;
        for (Property property : hiddenProperties) {
            if (property.getName().equals("prop1")) {
                if (property.getKey().equals("key2")) {
                    foundProp1Key2 = true;
                    assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                } else if (property.getKey().equals("key1")) {
                    if (property.getVisibility().equals(VISIBILITY_A)) {
                        foundProp1Key1VisA = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A));
                        assertTrue("should be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else if (property.getVisibility().equals(VISIBILITY_B)) {
                        foundProp1Key1VisB = true;
                        assertFalse("should not be hidden", property.isHidden(AUTHORIZATIONS_A_AND_B));
                    } else {
                        throw new RuntimeException("Unexpected visibility " + property.getVisibility());
                    }
                } else {
                    throw new RuntimeException("Unexpected property key " + property.getKey());
                }
            } else {
                throw new RuntimeException("Unexpected property name " + property.getName());
            }
        }
        assertTrue("Prop1Key2 not found", foundProp1Key2);
        assertTrue("Prop1Key1VisB not found", foundProp1Key1VisB);
        assertTrue("Prop1Key1VisA not found", foundProp1Key1VisA);

        v1.markPropertyVisible("key1", "prop1", VISIBILITY_A, VISIBILITY_A_AND_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Assert.assertEquals(3, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties("prop1")));
    }

    /**
     * This tests simulates two workspaces w1 (via A) and w1 (vis B).
     * Both w1 and w2 has e1 on it.
     * e1 is linked to e2.
     * What happens if w1 (vis A) marks e1 hidden, then deletes itself?
     */
    @Test
    public void testMarkVertexHiddenAndDeleteEdges() {
        Vertex w1 = graph.addVertex("w1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex w2 = graph.addVertex("w2", VISIBILITY_B, AUTHORIZATIONS_B);
        Vertex e1 = graph.addVertex("e1", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex e2 = graph.addVertex("e2", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        graph.addEdge("w1-e1", w1, e1, "test", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("w2-e1", w2, e1, "test", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge("e1-e2", e1, e2, "test", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getVertex("e1", AUTHORIZATIONS_EMPTY);
        graph.markVertexHidden(e1, VISIBILITY_A, AUTHORIZATIONS_EMPTY);
        graph.flush();

        graph.getVertex("w1", AUTHORIZATIONS_A);
        graph.deleteVertex("w1", AUTHORIZATIONS_A);
        graph.flush();

        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertEquals("e2", toList(graph.getVertices(AUTHORIZATIONS_A)).get(0).getId());

        Assert.assertEquals(3, count(graph.getVertices(AUTHORIZATIONS_B)));
        boolean foundW2 = false;
        boolean foundE1 = false;
        boolean foundE2 = false;
        for (Vertex v : graph.getVertices(AUTHORIZATIONS_B)) {
            if (v.getId().equals("w2")) {
                foundW2 = true;
            } else if (v.getId().equals("e1")) {
                foundE1 = true;
            } else if (v.getId().equals("e2")) {
                foundE2 = true;
            } else {
                throw new VertexiumException("Unexpected id: " + v.getId());
            }
        }
        assertTrue("w2", foundW2);
        assertTrue("e1", foundE1);
        assertTrue("e2", foundE2);
    }

    @Test
    public void testDeleteVertexWithProperties() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Property prop1 = v1.getProperty("prop1");

        Assert.assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.deleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));

        assertEvents(
                new AddVertexEvent(graph, v1),
                new AddPropertyEvent(graph, v1, prop1),
                new DeleteVertexEvent(graph, v1)
        );
    }

    @Test
    public void testAddEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge addedEdge = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();
        assertNotNull(addedEdge);
        assertEquals("e1", addedEdge.getId());
        assertEquals("label1", addedEdge.getLabel());
        assertEquals("v1", addedEdge.getVertexId(Direction.OUT));
        assertEquals(v1, addedEdge.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", addedEdge.getVertexId(Direction.IN));
        assertEquals(v2, addedEdge.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, addedEdge.getVisibility());

        graph.getVertex("v1", FetchHint.NONE, AUTHORIZATIONS_A);
        graph.getVertex("v1", FetchHint.ALL, AUTHORIZATIONS_A);
        graph.getVertex("v1", EnumSet.of(FetchHint.PROPERTIES), AUTHORIZATIONS_A);
        graph.getVertex("v1", FetchHint.EDGE_REFS, AUTHORIZATIONS_A);
        graph.getVertex("v1", EnumSet.of(FetchHint.IN_EDGE_REFS), AUTHORIZATIONS_A);
        graph.getVertex("v1", EnumSet.of(FetchHint.OUT_EDGE_REFS), AUTHORIZATIONS_A);

        graph.getEdge("e1", FetchHint.NONE, AUTHORIZATIONS_A);
        graph.getEdge("e1", FetchHint.ALL, AUTHORIZATIONS_A);
        graph.getEdge("e1", EnumSet.of(FetchHint.PROPERTIES), AUTHORIZATIONS_A);

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_B);
        assertNull(e);

        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals("label1", e.getLabel());
        assertEquals("v1", e.getVertexId(Direction.OUT));
        assertEquals(v1, e.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", e.getVertexId(Direction.IN));
        assertEquals(v2, e.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, e.getVisibility());

        graph.flush();
        assertEvents(
                new AddVertexEvent(graph, v1),
                new AddVertexEvent(graph, v2),
                new AddEdgeEvent(graph, addedEdge)
        );
    }

    @Test
    public void testGetEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to2label1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to2label2", v1, v2, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2to1", v2.getId(), v1.getId(), "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);

        Assert.assertEquals(3, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.BOTH, "label1", AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.OUT, "label1", AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, "label1", AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.OUT, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));

        Assert.assertArrayEquals(new String[]{"label1", "label2"}, IterableUtils.toArray(v1.getEdgeLabels(Direction.OUT, AUTHORIZATIONS_A), String.class));
        Assert.assertArrayEquals(new String[]{"label1"}, IterableUtils.toArray(v1.getEdgeLabels(Direction.IN, AUTHORIZATIONS_A), String.class));
        Assert.assertArrayEquals(new String[]{"label1", "label2"}, IterableUtils.toArray(v1.getEdgeLabels(Direction.BOTH, AUTHORIZATIONS_A), String.class));
    }

    @Test
    public void testGetEdgeVertexPairs() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v2_label1 = graph.addEdge("v1_to_v2_label1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v2_label2 = graph.addEdge("v1_to_v2_label2", v1, v2, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v3_label2 = graph.addEdge("v1_to_v3_label2", v1, v3, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);

        List<EdgeVertexPair> pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(3, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label1, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));

        pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, "label2", AUTHORIZATIONS_A));
        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));
    }

    @Test
    public void testAddEdgeWithProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge addedEdge = graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A)
                .setProperty("propA", "valueA", VISIBILITY_A)
                .setProperty("propB", "valueB", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        Assert.assertEquals(0, count(e.getPropertyValues("propB")));

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals("valueB", e.getPropertyValues("propB").iterator().next());
        assertEquals("valueA", e.getPropertyValue("propA"));
        assertEquals("valueB", e.getPropertyValue("propB"));

        graph.flush();
        assertEvents(
                new AddVertexEvent(graph, v1),
                new AddVertexEvent(graph, v2),
                new AddEdgeEvent(graph, addedEdge),
                new AddPropertyEvent(graph, addedEdge, addedEdge.getProperty("propA")),
                new AddPropertyEvent(graph, addedEdge, addedEdge.getProperty("propB"))
        );
    }

    @Test
    public void testAddEdgeWithNullInOutVertices() {
        try {
            String outVertexId = null;
            String inVertexId = null;
            getGraph().prepareEdge("e1", outVertexId, inVertexId, "label", VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }

        try {
            Vertex outVertex = null;
            Vertex inVertex = null;
            getGraph().prepareEdge("e1", outVertex, inVertex, "label", VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testAddEdgeWithNullLabels() {
        try {
            String label = null;
            getGraph().prepareEdge("e1", "v1", "v2", label, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }

        try {
            String label = null;
            Vertex outVertex = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
            Vertex inVertex = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
            getGraph().prepareEdge("e1", outVertex, inVertex, label, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testChangingPropertyOnEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A)
                .setProperty("propA", "valueA", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());

        Property propA = e.getProperty("", "propA");
        assertNotNull(propA);

        e.markPropertyHidden(propA, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(0, count(e.getProperties()));
        Assert.assertEquals(0, count(e.getPropertyValues("propA")));

        e.setProperty(propA.getName(), "valueA_changed", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA_changed", e.getPropertyValues("propA").iterator().next());

        e.markPropertyVisible(propA, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(e.getProperties()));
        Assert.assertEquals(2, count(e.getPropertyValues("propA")));

        List<Object> propertyValues = IterableUtils.toList(e.getPropertyValues("propA"));
        assertTrue(propertyValues.contains("valueA"));
        assertTrue(propertyValues.contains("valueA_changed"));
    }

    @Test
    public void testAlterEdgeLabel() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A)
                .setProperty("propA", "valueA", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals("label1", e.getLabel());
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        Assert.assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals("label1", IterableUtils.single(v1.getEdgeLabels(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals("label1", IterableUtils.single(v2.getEdgeLabels(Direction.IN, AUTHORIZATIONS_A)));

        e.prepareMutation()
                .alterEdgeLabel("label2")
                .save(AUTHORIZATIONS_A);
        graph.flush();
        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals("label2", e.getLabel());
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals("label2", IterableUtils.single(v1.getEdgeLabels(Direction.OUT, AUTHORIZATIONS_A)));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals("label2", IterableUtils.single(v2.getEdgeLabels(Direction.IN, AUTHORIZATIONS_A)));

        graph.prepareEdge(e.getId(), e.getVertexId(Direction.OUT), e.getVertexId(Direction.IN), e.getLabel(), e.getVisibility())
                .alterEdgeLabel("label3")
                .save(AUTHORIZATIONS_A);
        graph.flush();
        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals("label3", e.getLabel());
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals("label3", IterableUtils.single(v1.getEdgeLabels(Direction.OUT, AUTHORIZATIONS_A)));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals("label3", IterableUtils.single(v2.getEdgeLabels(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testDeleteEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge addedEdge = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        try {
            graph.deleteEdge("e1", AUTHORIZATIONS_B);
        } catch (NullPointerException e) {
            // expected
        }
        Assert.assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.deleteEdge("e1", AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));

        graph.flush();
        assertEvents(
                new AddVertexEvent(graph, v1),
                new AddVertexEvent(graph, v2),
                new AddEdgeEvent(graph, addedEdge),
                new DeleteEdgeEvent(graph, addedEdge)
        );
    }

    @Test
    public void testAddEdgeWithVisibility() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, "edgeB", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.flush();

        Iterable<Edge> aEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(aEdges));
        assertEquals("edgeA", IterableUtils.single(aEdges).getLabel());

        Iterable<Edge> bEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_B);
        Assert.assertEquals(1, count(bEdges));
        assertEquals("edgeB", IterableUtils.single(bEdges).getLabel());

        Iterable<Edge> allEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(allEdges));
    }

    @Test
    public void testGraphQuery() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, "edgeB", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(2, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).limit(1).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).limit(1).vertices();
        assertResultsCount(1, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).skip(2).vertices();
        assertResultsCount(0, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).limit(2).vertices();
        assertResultsCount(1, 2, vertices);

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A).edges();
        assertResultsCount(2, 2, edges);

        edges = graph.query(AUTHORIZATIONS_A).hasEdgeLabel("edgeA").edges();
        assertResultsCount(1, 1, edges);

        edges = graph.query(AUTHORIZATIONS_A).hasEdgeLabel("edgeA", "edgeB").edges();
        assertResultsCount(2, 2, edges);

        QueryResultsIterable<Element> elements = graph.query(AUTHORIZATIONS_A).elements();
        assertResultsCount(4, 4, elements);

        vertices = graph.query(AUTHORIZATIONS_A).has("name").vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasNot("name").vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("notSetProp").vertices();
        assertResultsCount(0, 0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasNot("notSetProp").vertices();
        assertResultsCount(2, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("notSetProp", Compare.NOT_EQUAL, 5).vertices();
        assertResultsCount(2, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("notSetProp", Compare.EQUAL, 5).vertices();
        assertResultsCount(0, 0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("notSetProp", Compare.LESS_THAN_EQUAL, 5).vertices();
        assertResultsCount(0, 0, vertices);
    }

    @Test
    public void testGraphQueryWithQueryString() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.setProperty("description", "This is vertex 1 - dog.", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v2.setProperty("description", "This is vertex 2 - cat.", VISIBILITY_B, AUTHORIZATIONS_ALL);
        Edge e1 = graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        e1.setProperty("description", "This is edge 1 - dog to cat.", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        Iterable<Vertex> vertices = graph.query("vertex", AUTHORIZATIONS_A_AND_B).vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query("vertex", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("dog", AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("dog", AUTHORIZATIONS_B).vertices();
        Assert.assertEquals(0, count(vertices));

        Iterable<Element> elements = graph.query("dog", AUTHORIZATIONS_A_AND_B).elements();
        Assert.assertEquals(2, count(elements));
    }

    @Test
    public void testGraphQueryWithQueryStringWithAuthorizations() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.setProperty("description", "This is vertex 1 - dog.", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_ALL);
        v2.setProperty("description", "This is vertex 2 - cat.", VISIBILITY_B, AUTHORIZATIONS_ALL);
        Edge e1 = graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        e1.setProperty("edgeDescription", "This is edge 1 - dog to cat.", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));
        if (isIterableWithTotalHitsSupported(vertices)) {
            IterableWithTotalHits hits = (IterableWithTotalHits) vertices;
            assertEquals(1, hits.getTotalHits());
        }

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A).edges();
        assertEquals(1, count(edges));
    }

    protected boolean isIterableWithTotalHitsSupported(Iterable<Vertex> vertices) {
        return vertices instanceof IterableWithTotalHits;
    }

    @Test
    public void testGraphQueryHas() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "hello", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text", "world", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age")
                .vertices();
        Assert.assertEquals(2, count(vertices));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .hasNot("age")
                    .vertices();
            Assert.assertEquals(0, count(vertices));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .has("birthDate", Compare.EQUAL, createDate(1989, 1, 5))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("hello", AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .has("birthDate", Compare.EQUAL, createDate(1989, 1, 5))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("birthDate", Compare.EQUAL, createDate(1989, 1, 5))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("lastAccessed", Compare.EQUAL, createDate(2014, 2, 24, 13, 0, 5))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(25, (int) toList(vertices).get(0).getPropertyValue("age"));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .hasNot("age", 25)
                    .vertices();
            Assert.assertEquals(1, count(vertices));
            Assert.assertEquals(30, (int) toList(vertices).get(0).getPropertyValue("age"));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN_EQUAL, 25)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Contains.IN, new Integer[]{25})
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(25, (int) toList(vertices).get(0).getPropertyValue("age"));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .has("age", Contains.NOT_IN, new Integer[]{25})
                    .vertices();
            Assert.assertEquals(1, count(vertices));
            Assert.assertEquals(30, (int) toList(vertices).get(0).getPropertyValue("age"));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Contains.IN, new Integer[]{25, 30})
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.LESS_THAN, 26)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.LESS_THAN_EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.NOT_EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("lastAccessed", Compare.EQUAL, new DateOnly(2014, 2, 24))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("*", AUTHORIZATIONS_A)
                .has("age", Contains.IN, new Integer[]{25, 30})
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = new CompositeGraphQuery(
                graph.query(AUTHORIZATIONS_A).has("age", 25),
                graph.query(AUTHORIZATIONS_A).has("age", 25),
                graph.query(AUTHORIZATIONS_A).has("age", 30)
        ).vertices();
        Assert.assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQueryHasGeoPointAndExact() {
        graph.defineProperty("location").dataType(GeoPoint.class).define();
        graph.defineProperty("exact").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "val1", VISIBILITY_A)
                .setProperty("exact", "val1", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("prop2", "val2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Element> results = graph.query("*", AUTHORIZATIONS_A_AND_B).has("prop1").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());

        results = graph.query("*", AUTHORIZATIONS_A_AND_B).has("exact").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());

        results = graph.query("*", AUTHORIZATIONS_A_AND_B).has("location").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v1", results.iterator().next().getId());
    }

    @Test
    public void testGraphQueryContainsNotIn() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("status", "0", VISIBILITY_A)
                .setProperty("name", "susan", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("status", "1", VISIBILITY_A)
                .setProperty("name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v4", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v5", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v6", VISIBILITY_A)
                .setProperty("status", "0", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        try {
            Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                    .has("status", Contains.NOT_IN, new String[]{"0"})
                    .vertices();
            Assert.assertEquals(4, count(vertices));

            vertices = graph.query(AUTHORIZATIONS_A)
                    .has("status", Contains.NOT_IN, new String[]{"0", "1"})
                    .vertices();
            Assert.assertEquals(3, count(vertices));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }
    }

    @Test
    public void testGraphQueryHasNotGeoPointAndExact() {
        graph.defineProperty("location").dataType(GeoPoint.class).define();
        graph.defineProperty("exact").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "val1", VISIBILITY_A)
                .setProperty("exact", "val1", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("prop2", "val2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Element> results = graph.query("*", AUTHORIZATIONS_A_AND_B).hasNot("prop1").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());

        results = graph.query("*", AUTHORIZATIONS_A_AND_B).hasNot("prop3").sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING).elements();
        assertEquals(2, count(results));
        Iterator<Element> iterator = results.iterator();
        assertEquals("v1", iterator.next().getId());
        assertEquals("v2", iterator.next().getId());

        results = graph.query("*", AUTHORIZATIONS_A_AND_B).hasNot("exact").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());

        results = graph.query("*", AUTHORIZATIONS_A_AND_B).hasNot("location").elements();
        assertEquals(1, count(results));
        assertEquals(1, ((IterableWithTotalHits) results).getTotalHits());
        assertEquals("v2", results.iterator().next().getId());
    }

    @Test
    public void testGraphQueryHasTwoVisibilities() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "v2", VISIBILITY_A)
                .addPropertyValue("k1", "age", 30, VISIBILITY_A)
                .addPropertyValue("k2", "age", 35, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("name", "v3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age")
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .hasNot("age")
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryIn() {
        graph.defineProperty("age").dataType(Integer.class).sortable(true).define();
        graph.defineProperty("name").dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "joe ferner", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "bob smith", VISIBILITY_B)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("name", "tom thumb", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<String> strings = new ArrayList<>();
        strings.add("joe ferner");
        strings.add("tom thumb");
        Iterable<Vertex> results = graph.query(AUTHORIZATIONS_A_AND_B).has("name", Contains.IN, strings).vertices();
        assertEquals(2, ((IterableWithTotalHits) results).getTotalHits());
        List<Vertex> vertices = toList(results);
        assertEquals(2, vertices.size());
        assertEquals("v1", vertices.get(0).getId());
        assertEquals("v3", vertices.get(1).getId());
    }

    @Test
    public void testGraphQuerySort() {
        graph.defineProperty("age").dataType(Integer.class).sortable(true).define();
        graph.defineProperty("name").dataType(String.class).sortable(true).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "bob", VISIBILITY_B)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("name", "tom", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_A)
                .setProperty("name", "tom", VISIBILITY_A)
                .setProperty("age", 35, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", "label2", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", "v1", "v2", "label1", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", "v1", "v2", "label3", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("age", SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v2", "v3", "v4", "v1"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("age", SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v4", "v3", "v2", "v1"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("name", SortDirection.ASCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));
        assertEquals("v2", vertices.get(0).getId());
        assertEquals("v1", vertices.get(1).getId());
        assertTrue(vertices.get(2).getId().equals("v3") || vertices.get(2).getId().equals("v4"));
        assertTrue(vertices.get(3).getId().equals("v3") || vertices.get(3).getId().equals("v4"));

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("name", SortDirection.DESCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));
        assertTrue(vertices.get(0).getId().equals("v3") || vertices.get(0).getId().equals("v4"));
        assertTrue(vertices.get(1).getId().equals("v3") || vertices.get(1).getId().equals("v4"));
        assertEquals("v1", vertices.get(2).getId());
        assertEquals("v2", vertices.get(3).getId());

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("name", SortDirection.ASCENDING)
                .sort("age", SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v2", "v1", "v3", "v4"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("name", SortDirection.ASCENDING)
                .sort("age", SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v2", "v1", "v4", "v3"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v1", "v2", "v3", "v4"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, new String[]{"v4", "v3", "v2", "v1"});

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("otherfield", SortDirection.ASCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));

        List<Edge> edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.LABEL_PROPERTY_NAME, SortDirection.ASCENDING)
                .edges());
        assertEdgeIds(edges, new String[]{"e2", "e1", "e3"});

        edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.LABEL_PROPERTY_NAME, SortDirection.DESCENDING)
                .edges());
        assertEdgeIds(edges, new String[]{"e3", "e1", "e2"});
    }

    @Test
    public void testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex() {
        graph.defineProperty("age").dataType(Integer.class).sortable(true).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "bob", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        QueryResultsIterable<Vertex> vertices
                = graph.query(AUTHORIZATIONS_A).sort("age", SortDirection.ASCENDING).vertices();
        Assert.assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQuerySortOnPropertyWhichIsFullTextAndExactMatchIndexed() {
        graph.defineProperty("name")
                .dataType(String.class)
                .sortable(true)
                .textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT)
                .define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "1-2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "1-1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("name", "3-1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        QueryResultsIterable<Vertex> vertices
                = graph.query(AUTHORIZATIONS_A_AND_B).sort("name", SortDirection.ASCENDING).vertices();
        assertVertexIds(vertices, new String[]{"v2", "v1", "v3"});

        vertices = graph.query("3", AUTHORIZATIONS_A_AND_B).vertices();
        assertVertexIds(vertices, new String[]{"v3"});

        vertices = graph.query("*", AUTHORIZATIONS_A_AND_B)
                .has("name", Compare.EQUAL, "3-1")
                .vertices();
        assertVertexIds(vertices, new String[]{"v3"});
    }

    @Test
    public void testGraphQueryVertexHasWithSecurity() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));
        if (vertices instanceof IterableWithTotalHits) {
            Assert.assertEquals(1, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = graph.query(AUTHORIZATIONS_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(0, count(vertices)); // need auth A to see the v2 node itself
        if (vertices instanceof IterableWithTotalHits) {
            Assert.assertEquals(0, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(2, count(vertices));
        if (vertices instanceof IterableWithTotalHits) {
            Assert.assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());
        }
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityGranularity() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("description", "v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("description", "v2", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .vertices();
        boolean hasAgeVisA = false;
        boolean hasAgeVisB = false;
        for (Vertex v : vertices) {
            Property prop = v.getProperty("age");
            if (prop == null) {
                continue;
            }
            if ((Integer) prop.getValue() == 25) {
                if (prop.getVisibility().equals(VISIBILITY_A)) {
                    hasAgeVisA = true;
                } else if (prop.getVisibility().equals(VISIBILITY_B)) {
                    hasAgeVisB = true;
                }
            }
        }
        assertEquals(2, count(vertices));
        assertTrue("has a", hasAgeVisA);
        assertFalse("has b", hasAgeVisB);

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityComplexFormula() {
        graph.prepareVertex("v1", VISIBILITY_MIXED_CASE_a)
                .setProperty("age", 25, VISIBILITY_MIXED_CASE_a)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_MIXED_CASE_a_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGetVertexWithBadAuthorizations() {
        graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        graph.flush();

        try {
            graph.getVertex("v1", AUTHORIZATIONS_BAD);
            throw new RuntimeException("Should throw " + SecurityVertexiumException.class.getSimpleName());
        } catch (SecurityVertexiumException ex) {
            // ok
        }
    }

    @Test
    public void testGraphQueryVertexNoVisibility() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("text", "hello", VISIBILITY_EMPTY)
                .setProperty("age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query("hello", AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("hello", AUTHORIZATIONS_A_AND_B)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryVertexWithVisibilityChange() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));

        // change to same visibility
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1 = v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(VISIBILITY_EMPTY, v1.getVisibility());

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityCantSeeVertex() {
        graph.prepareVertex("v1", VISIBILITY_B)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(0, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurityCantSeeProperty() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(0, count(vertices));
    }

    @Test
    public void testGraphQueryEdgeHasWithSecurity() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);

        graph.prepareEdge("e1", v1, v2, "edge", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", v1, v3, "edge", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .edges();
        Assert.assertEquals(1, count(edges));
    }

    @Test
    public void testGraphQueryUpdateVertex() throws NoSuchFieldException, IllegalAccessException {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.addEdge("v2tov3", v2, v3, "", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        boolean disableUpdateEdgeCountInSearchIndexSuccess = disableUpdateEdgeCountInSearchIndex(graph);

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("name", "Joe", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .setProperty("name", "Bob", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .setProperty("name", "Same", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Vertex> allVertices = toList(graph.query(AUTHORIZATIONS_A_AND_B).vertices());
        Assert.assertEquals(3, count(allVertices));
        if (disableUpdateEdgeCountInSearchIndexSuccess) {
            assertEquals(
                    "if edge indexing was disabled and updating vertices does not destroy the edge counts " +
                            "that were already in place 'v1' should be last since it has no edges",
                    "v1", allVertices.get(2).getId()
            );
        }

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("name", Compare.EQUAL, "Joe")
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .has("name", Compare.EQUAL, "Joe")
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testQueryWithVertexIds() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("age", 35, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<Vertex> vertices = toList(graph.query(new String[]{"v1", "v2"}, AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN, 27)
                .vertices());
        Assert.assertEquals(1, vertices.size());
        assertEquals("v2", vertices.get(0).getId());

        vertices = toList(graph.query(new String[]{"v1", "v2", "v3"}, AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN, 27)
                .vertices());
        Assert.assertEquals(2, vertices.size());
        List<String> vertexIds = toList(new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        });
        Assert.assertTrue("v2 not found", vertexIds.contains("v2"));
        Assert.assertTrue("v3 not found", vertexIds.contains("v3"));
    }

    @Test
    public void testDisableEdgeIndexing() throws NoSuchFieldException, IllegalAccessException {
        if (!disableEdgeIndexing(graph)) {
            LOGGER.info("skipping %s doesn't support disabling index", SearchIndex.class.getSimpleName());
            return;
        }

        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);

        graph.prepareEdge("e1", v1, v2, "edge", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .has("prop1", "value1")
                .edges();
        Assert.assertEquals(0, count(edges));
    }

    @Test
    public void testGraphQueryHasWithSpaces() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "Joe Ferner", VISIBILITY_A)
                .setProperty("propWithNonAlphaCharacters", "hyphen-word, etc.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "Joe Smith", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query("Ferner", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("joe", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        if (isLuceneQueriesSupported()) {
            vertices = graph.query("joe AND ferner", AUTHORIZATIONS_A)
                    .vertices();
            Assert.assertEquals(1, count(vertices));
        }

        if (isLuceneQueriesSupported()) {
            vertices = graph.query("joe smith", AUTHORIZATIONS_A)
                    .vertices();
            List<Vertex> verticesList = toList(vertices);
            assertEquals(2, verticesList.size());
            boolean foundV1 = false;
            boolean foundV2 = false;
            for (Vertex v : verticesList) {
                if (v.getId().equals("v1")) {
                    foundV1 = true;
                } else if (v.getId().equals("v2")) {
                    foundV2 = true;
                } else {
                    throw new RuntimeException("Invalid vertex id: " + v.getId());
                }
            }
            assertTrue(foundV1);
            assertTrue(foundV2);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Ferner")
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe")
                .has("name", TextPredicate.CONTAINS, "Ferner")
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe Ferner")
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("propWithNonAlphaCharacters", TextPredicate.CONTAINS, "hyphen-word, etc.")
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryWithANDOperatorAndWithExactMatchFields() {
        graph.defineProperty("firstName").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("firstName", "Joe", VISIBILITY_A)
                .setProperty("lastName", "Ferner", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("firstName", "Joe", VISIBILITY_A)
                .setProperty("lastName", "Smith", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        if (isLuceneQueriesSupported() && isLuceneAndQueriesSupported()) {
            Iterable<Vertex> vertices = graph.query("Joe AND ferner", AUTHORIZATIONS_A)
                    .vertices();
            Assert.assertEquals(1, count(vertices));
        }
    }

    @Test
    public void testGraphQueryHasWithSpacesAndFieldedQueryString() {
        if (!isFieldNamesInQuerySupported()) {
            return;
        }

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "Joe Ferner", VISIBILITY_A)
                .setProperty("propWithHyphen", "hyphen-word", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "Joe Smith", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        if (isLuceneQueriesSupported()) {
            Iterable<Vertex> vertices = graph.query("name:\"joe ferner\"", AUTHORIZATIONS_A)
                    .vertices();
            Assert.assertEquals(1, count(vertices));
        }
    }

    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    protected boolean isLuceneQueriesSupported() {
        return !(graph.query(AUTHORIZATIONS_A) instanceof DefaultGraphQuery);
    }

    protected boolean isLuceneAndQueriesSupported() {
        return !(graph.query(AUTHORIZATIONS_A) instanceof DefaultGraphQuery);
    }

    @Test
    public void testStoreGeoPoint() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9544, -77.3464, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 1))
                .vertices());
        Assert.assertEquals(1, count(vertices));
        GeoPoint geoPoint = (GeoPoint) vertices.get(0).getPropertyValue("location");
        assertEquals(38.9186, geoPoint.getLatitude(), 0.001);
        assertEquals(-77.2297, geoPoint.getLongitude(), 0.001);
        assertEquals("Reston, VA", geoPoint.getDescription());

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 25))
                .vertices());
        Assert.assertEquals(2, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoRect(new GeoPoint(39, -78), new GeoPoint(38, -77)))
                .vertices());
        Assert.assertEquals(2, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoHash(38.9186, -77.2297, 2))
                .vertices());
        Assert.assertEquals(2, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoHash(38.9186, -77.2297, 3))
                .vertices());
        Assert.assertEquals(1, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", TextPredicate.CONTAINS, "Reston")
                .vertices());
        Assert.assertEquals(2, count(vertices));
    }

    @Test
    public void testStoreGeoCircle() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("location", new GeoCircle(38.9186, -77.2297, 100, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .has("location", GeoCompare.WITHIN, new GeoCircle(38.92, -77.23, 10))
                .vertices());
        Assert.assertEquals(1, count(vertices));
        GeoCircle geoCircle = (GeoCircle) vertices.get(0).getPropertyValue("location");
        assertEquals(38.9186, geoCircle.getLatitude(), 0.001);
        assertEquals(-77.2297, geoCircle.getLongitude(), 0.001);
        assertEquals(100.0, geoCircle.getRadius(), 0.001);
        assertEquals("Reston, VA", geoCircle.getDescription());
    }

    private Date createDate(int year, int month, int day) {
        return new GregorianCalendar(year, month, day).getTime();
    }

    private Date createDate(int year, int month, int day, int hour, int min, int sec) {
        return new GregorianCalendar(year, month, day, hour, min, sec).getTime();
    }

    @Test
    public void testGraphQueryRange() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 20, 29)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, 30)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, true, 30, false)
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(25, toList(vertices).get(0).getPropertyValue("age"));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, false, 30, true)
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(30, toList(vertices).get(0).getPropertyValue("age"));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, true, 30, true)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, false, 30, false)
                .vertices();
        Assert.assertEquals(0, count(vertices));
    }

    @Test
    public void testVertexQuery() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.setProperty("prop1", "value1", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v2.setProperty("prop1", "value2", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v3.setProperty("prop1", "value3", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Edge ev1v2 = graph.prepareEdge("e v1->v2", v1, v2, "edgeA", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge ev1v3 = graph.prepareEdge("e v1->v3", v1, v3, "edgeB", VISIBILITY_A)
                .setProperty("prop1", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge ev2v3 = graph.prepareEdge("e v2->v3", v2, v3, "edgeB", VISIBILITY_A)
                .setProperty("prop1", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Vertex> vertices = v1.query(AUTHORIZATIONS_A).vertices();
        Assert.assertEquals(2, count(vertices));
        org.vertexium.test.util.IterableUtils.assertContains(v2, vertices);
        org.vertexium.test.util.IterableUtils.assertContains(v3, vertices);
        if (isIterableWithTotalHitsSupported(vertices)) {
            Assert.assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());

            vertices = v1.query(AUTHORIZATIONS_A).limit(1).vertices();
            Assert.assertEquals(1, count(vertices));
            Assert.assertEquals(2, ((IterableWithTotalHits) vertices).getTotalHits());
        }

        vertices = v1.query(AUTHORIZATIONS_A)
                .has("prop1", "value2")
                .vertices();
        Assert.assertEquals(1, count(vertices));
        org.vertexium.test.util.IterableUtils.assertContains(v2, vertices);

        Iterable<Edge> edges = v1.query(AUTHORIZATIONS_A).edges();
        Assert.assertEquals(2, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(AUTHORIZATIONS_A).hasEdgeLabel("edgeA", "edgeB").edges();
        Assert.assertEquals(2, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(AUTHORIZATIONS_A).hasEdgeLabel("edgeA").edges();
        Assert.assertEquals(1, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);

        vertices = v1.query(AUTHORIZATIONS_A).hasEdgeLabel("edgeA").vertices();
        Assert.assertEquals(1, count(vertices));
        org.vertexium.test.util.IterableUtils.assertContains(v2, vertices);
    }

    @Test
    public void testFindPaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v6 = graph.addVertex("v6", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v2
        graph.addEdge(v2, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v4
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v3, v5, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v5
        graph.addEdge(v4, v6, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v6
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        List<Path> paths = toList(graph.findPaths("v1", "v4", 2, AUTHORIZATIONS_A));
        List<Path> pathsByLabels = toList(graph.findPaths("v1", "v4", new String[]{"knows"}, 2, AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        List<Path> pathsByBadLabel = toList(graph.findPaths("v1", "v4", new String[]{"bad"}, 2, AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());

        // v1 -> v2 -> v4
        // v1 -> v3 -> v4
        assertEquals(2, paths.size());
        boolean found2 = false;
        boolean found3 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (String id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v2.getId().equals(id)) {
                        found2 = true;
                    } else if (v3.getId().equals(id)) {
                        found3 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v4.getId());
                }
                i++;
            }
        }
        assertTrue("v2 not found in path", found2);
        assertTrue("v3 not found in path", found3);

        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        paths = toList(graph.findPaths("v4", "v1", 2, AUTHORIZATIONS_A));
        pathsByLabels = toList(graph.findPaths("v4", "v1", new String[]{"knows"}, 2, AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toList(graph.findPaths("v4", "v1", new String[]{"bad"}, 2, AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        // v4 -> v2 -> v1
        // v4 -> v3 -> v1
        assertEquals(2, paths.size());
        found2 = false;
        found3 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (String id : path) {
                if (i == 0) {
                    assertEquals(id, v4.getId());
                } else if (i == 1) {
                    if (v2.getId().equals(id)) {
                        found2 = true;
                    } else if (v3.getId().equals(id)) {
                        found3 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v1.getId());
                }
                i++;
            }
        }
        assertTrue("v2 not found in path", found2);
        assertTrue("v3 not found in path", found3);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v6 = graph.getVertex("v6", AUTHORIZATIONS_A);
        paths = toList(graph.findPaths("v1", "v6", 3, AUTHORIZATIONS_A));
        pathsByLabels = toList(graph.findPaths("v1", "v6", new String[]{"knows"}, 3, AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toList(graph.findPaths("v1", "v6", new String[]{"bad"}, 3, AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        // v1 -> v2 -> v4 -> v6
        // v1 -> v3 -> v4 -> v6
        assertEquals(2, paths.size());
        found2 = false;
        found3 = false;
        for (Path path : paths) {
            assertEquals(4, path.length());
            int i = 0;
            for (String id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v2.getId().equals(id)) {
                        found2 = true;
                    } else if (v3.getId().equals(id)) {
                        found3 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v4.getId());
                } else if (i == 3) {
                    assertEquals(id, v6.getId());
                }
                i++;
            }
        }
        assertTrue("v2 not found in path", found2);
        assertTrue("v3 not found in path", found3);
    }

    @Test
    public void testFindPathsWithSoftDeletedEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, "knows", VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v1 -> v2
        Edge v2ToV3 = graph.addEdge(v2, v3, "knows", VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v2 -> v3
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v3 = graph.getVertex("v3", AUTHORIZATIONS_A);
        List<Path> paths = toList(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A));

        // v1 -> v2 -> v3
        assertEquals(1, paths.size());
        for (Path path : paths) {
            assertEquals(3, path.length());
            assertEquals(path.get(0), v1.getId());
            assertEquals(path.get(1), v2.getId());
            assertEquals(path.get(2), v3.getId());
        }

        graph.softDeleteEdge(v2ToV3, AUTHORIZATIONS_A);
        graph.flush();

        assertNull(graph.getEdge(v2ToV3.getId(), AUTHORIZATIONS_A));
        paths = toList(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A));
        assertEquals(0, paths.size());
    }

    @Test
    public void testFindPathsWithHiddenEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.addEdge(v1, v2, "knows", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v1 -> v2
        Edge v2ToV3 = graph.addEdge(v2, v3, "knows", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v2 -> v3
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v3 = graph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
        List<Path> paths = toList(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A_AND_B));

        // v1 -> v2 -> v3
        assertEquals(1, paths.size());
        for (Path path : paths) {
            assertEquals(3, path.length());
            assertEquals(path.get(0), v1.getId());
            assertEquals(path.get(1), v2.getId());
            assertEquals(path.get(2), v3.getId());
        }

        graph.markEdgeHidden(v2ToV3, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        assertNull(graph.getEdge(v2ToV3.getId(), AUTHORIZATIONS_A_AND_B));
        paths = toList(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_A));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths("v1", "v3", 2, AUTHORIZATIONS_B));
        assertEquals(1, paths.size());
    }

    @Test
    public void testFindPathsMultiplePaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v4
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v2, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        graph.addEdge(v4, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        graph.addEdge(v2, v5, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        v5 = graph.getVertex("v5", AUTHORIZATIONS_A);

        List<Path> paths = toList(graph.findPaths("v1", "v2", 2, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2
        // v1 -> v3 -> v2
        assertEquals(2, paths.size());
        boolean found3 = false;
        boolean found4 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (String id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v3.getId().equals(id)) {
                        found3 = true;
                    } else if (v4.getId().equals(id)) {
                        found4 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v2.getId());
                }
                i++;
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);

        paths = toList(graph.findPaths("v1", "v2", 3, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2
        // v1 -> v3 -> v2
        // v1 -> v3 -> v4 -> v2
        // v1 -> v4 -> v3 -> v2
        assertEquals(4, paths.size());
        found3 = false;
        found4 = false;
        for (Path path : paths) {
            if (path.length() == 3) {
                int i = 0;
                for (String id : path) {
                    if (i == 0) {
                        assertEquals(id, v1.getId());
                    } else if (i == 1) {
                        if (v3.getId().equals(id)) {
                            found3 = true;
                        } else if (v4.getId().equals(id)) {
                            found4 = true;
                        } else {
                            fail("center of path is neither v2 or v3 but found " + id);
                        }
                    } else if (i == 2) {
                        assertEquals(id, v2.getId());
                    }
                    i++;
                }
            } else if (path.length() == 4) {
                assertTrue(true);
            } else {
                fail("Invalid path length " + path.length());
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);

        paths = toList(graph.findPaths("v1", "v5", 2, AUTHORIZATIONS_A));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths("v1", "v5", 3, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2 -> v5
        // v1 -> v3 -> v2 -> v5
        assertEquals(2, paths.size());
        found3 = false;
        found4 = false;
        for (Path path : paths) {
            assertEquals(4, path.length());
            int i = 0;
            for (String id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v3.getId().equals(id)) {
                        found3 = true;
                    } else if (v4.getId().equals(id)) {
                        found4 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v2.getId());
                } else if (i == 3) {
                    assertEquals(id, v5.getId());
                }
                i++;
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);
    }

    @Test
    public void testGetVerticesFromVertex() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge(v1, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v5, "knows", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(3, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v1.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(2, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v2.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v2.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v3 = graph.getVertex("v3", AUTHORIZATIONS_A);
        Assert.assertEquals(2, count(v3.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v3.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v3.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v4.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v4.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v4.getVertices(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testGetVertexIdsFromVertex() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge(v1, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v5, "knows", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(3, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v1.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(3, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v2.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v2.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v3 = graph.getVertex("v3", AUTHORIZATIONS_A);
        Assert.assertEquals(2, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v3.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v3.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));

        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v4.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(0, count(v4.getVertexIds(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v4.getVertexIds(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testBlankVisibilityString() {
        Vertex v = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        graph.flush();

        v = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_EMPTY, v.getVisibility());
    }

    @Test
    public void testElementMutationDoesntChangeObjectUntilSave() throws InterruptedException {
        Vertex v = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        v.setProperty("prop1", "value1-1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        ElementMutation<Vertex> m = v.prepareMutation()
                .setProperty("prop1", "value1-2", VISIBILITY_A)
                .setProperty("prop2", "value2-2", VISIBILITY_A);
        Assert.assertEquals(1, count(v.getProperties()));
        assertEquals("value1-1", v.getPropertyValue("prop1"));

        v = m.save(AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(v.getProperties()));
        assertEquals("value1-2", v.getPropertyValue("prop1"));
        assertEquals("value2-2", v.getPropertyValue("prop2"));
    }

    @Test
    public void testFindRelatedEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = graph.addEdge("e v2->v3", v2, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = graph.addEdge("e v3->v1", v3, v1, "", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, "", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        Iterable<String> edgeIds = toList(graph.findRelatedEdgeIds(vertexIds, AUTHORIZATIONS_A));
        Assert.assertEquals(4, count(edgeIds));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev2v3.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev3v1.getId(), edgeIds);
    }

    @Test
    public void testFindRelatedEdgeIdsForVertices() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = graph.addEdge("e v2->v3", v2, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = graph.addEdge("e v3->v1", v3, v1, "", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, "", VISIBILITY_A, AUTHORIZATIONS_A);

        List<Vertex> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        Iterable<String> edgeIds = toList(graph.findRelatedEdgeIdsForVertices(vertices, AUTHORIZATIONS_A));
        Assert.assertEquals(4, count(edgeIds));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev2v3.getId(), edgeIds);
        org.vertexium.test.util.IterableUtils.assertContains(ev3v1.getId(), edgeIds);
    }

    @Test
    public void testFindRelatedEdgeSummary() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v1->v2", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v1->v3", v1, v3, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v2->v3", v2, v3, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v1", v3, v1, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, "", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(4, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", "label1", v1.getId(), v2.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v3", "label1", v1.getId(), v3.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v2->v3", "label2", v2.getId(), v3.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v3->v1", "label2", v3.getId(), v1.getId()), relatedEdges);
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterSoftDelete() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e v1->v2", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", "label1", v1.getId(), v2.getId()), relatedEdges);

        graph.softDeleteEdge(e1, AUTHORIZATIONS_A);
        graph.flush();

        relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterMarkedHidden() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e v1->v2", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", "label1", v1.getId(), v2.getId()), relatedEdges);

        graph.markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }

    // Test for performance
    //@Test
    @SuppressWarnings("unused")
    private void testFindRelatedEdgesPerformance() {
        int totalNumberOfVertices = 100;
        int totalNumberOfEdges = 10000;
        int totalVerticesToCheck = 100;

        Date startTime, endTime;
        Random random = new Random(100);

        startTime = new Date();
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < totalNumberOfVertices; i++) {
            vertices.add(graph.addVertex("v" + i, VISIBILITY_A, AUTHORIZATIONS_A));
        }
        graph.flush();
        endTime = new Date();
        long insertVerticesTime = endTime.getTime() - startTime.getTime();

        startTime = new Date();
        for (int i = 0; i < totalNumberOfEdges; i++) {
            Vertex outVertex = vertices.get(random.nextInt(vertices.size()));
            Vertex inVertex = vertices.get(random.nextInt(vertices.size()));
            graph.addEdge("e" + i, outVertex, inVertex, "", VISIBILITY_A, AUTHORIZATIONS_A);
        }
        graph.flush();
        endTime = new Date();
        long insertEdgesTime = endTime.getTime() - startTime.getTime();

        List<String> vertexIds = new ArrayList<>();
        for (int i = 0; i < totalVerticesToCheck; i++) {
            Vertex v = vertices.get(random.nextInt(vertices.size()));
            vertexIds.add(v.getId());
        }

        startTime = new Date();
        Iterable<String> edgeIds = toList(graph.findRelatedEdgeIds(vertexIds, AUTHORIZATIONS_A));
        count(edgeIds);
        endTime = new Date();
        long findRelatedEdgesTime = endTime.getTime() - startTime.getTime();

        LOGGER.info(
                "RESULTS\ntotalNumberOfVertices,totalNumberOfEdges,totalVerticesToCheck,insertVerticesTime,insertEdgesTime,findRelatedEdgesTime\n%d,%d,%d,%d,%d,%d",
                totalNumberOfVertices,
                totalNumberOfEdges,
                totalVerticesToCheck,
                insertVerticesTime,
                insertEdgesTime,
                findRelatedEdgesTime
        );
    }

    @Test
    public void testFilterEdgeIdsByAuthorization() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Metadata metadataPropB = new Metadata();
        metadataPropB.add("meta1", "meta1", VISIBILITY_A);
        graph.prepareEdge("e1", v1, v2, "label", VISIBILITY_A)
                .setProperty("propA", "propA", VISIBILITY_A)
                .setProperty("propB", "propB", VISIBILITY_A_AND_B)
                .setProperty("propBmeta", "propBmeta", metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        List<String> edgeIds = new ArrayList<>();
        edgeIds.add("e1");
        List<String> foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_B_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_B_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("e1", foundEdgeIds);

        foundEdgeIds = toList(graph.filterEdgeIdsByAuthorization(edgeIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        assertEquals(0, foundEdgeIds.size());
    }

    @Test
    public void testFilterVertexIdsByAuthorization() {
        Metadata metadataPropB = new Metadata();
        metadataPropB.add("meta1", "meta1", VISIBILITY_A);
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propA", "propA", VISIBILITY_A)
                .setProperty("propB", "propB", VISIBILITY_A_AND_B)
                .setProperty("propBmeta", "propBmeta", metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        List<String> foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_B_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, ElementFilter.ALL, AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_B_STRING, EnumSet.of(ElementFilter.ELEMENT), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_A_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        org.vertexium.test.util.IterableUtils.assertContains("v1", foundVertexIds);

        foundVertexIds = toList(graph.filterVertexIdsByAuthorization(vertexIds, VISIBILITY_C_STRING, EnumSet.of(ElementFilter.PROPERTY_METADATA), AUTHORIZATIONS_ALL));
        assertEquals(0, foundVertexIds.size());
    }

    @Test
    public void testEmptyPropertyMutation() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.prepareMutation().save(AUTHORIZATIONS_ALL);
    }

    @Test
    public void testTextIndex() throws Exception {
        graph.defineProperty("none").dataType(String.class).textIndexHint(TextIndexHint.NONE).define();
        graph.defineProperty("none").dataType(String.class).textIndexHint(TextIndexHint.NONE).define(); // try calling define twice
        graph.defineProperty("both").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("fullText").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("exactMatch").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("none", "Test Value", VISIBILITY_A)
                .setProperty("both", "Test Value", VISIBILITY_A)
                .setProperty("fullText", "Test Value", VISIBILITY_A)
                .setProperty("exactMatch", "Test Value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("Test Value", v1.getPropertyValue("none"));
        assertEquals("Test Value", v1.getPropertyValue("both"));
        assertEquals("Test Value", v1.getPropertyValue("fullText"));
        assertEquals("Test Value", v1.getPropertyValue("exactMatch"));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("both", TextPredicate.CONTAINS, "Test").vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("fullText", TextPredicate.CONTAINS, "Test").vertices()));
        Assert.assertEquals("exact match shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("exactMatch", "Test").vertices()));
        Assert.assertEquals("un-indexed property shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("none", "Test").vertices()));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("both", "Test Value").vertices()));
        Assert.assertEquals("default has predicate is equals which shouldn't work for full text", 0, count(graph.query(AUTHORIZATIONS_A).has("fullText", "Test Value").vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("exactMatch", "Test Value").vertices()));
        if (count(graph.query(AUTHORIZATIONS_A).has("none", "Test Value").vertices()) != 0) {
            LOGGER.warn("default has predicate is equals which shouldn't work for un-indexed");
        }
    }

    @Test
    public void testTextIndexDoesNotContain() throws Exception {
        graph.defineProperty("both").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("fullText").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("exactMatch").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("exactMatch", "Test Value", VISIBILITY_A)
                .setProperty("both", "Test123", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("both", "Test Value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("both", "Temp", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v4", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v5", VISIBILITY_A)
                .setProperty("both", "Test123 test", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("both", TextPredicate.DOES_NOT_CONTAIN, "Test")
                .vertices();
        Assert.assertEquals(3, count(vertices));
        List<String> expectedVertexIds = Arrays.asList("v1", "v3", "v4");
        for (Vertex v : vertices) {
            assertTrue(expectedVertexIds.contains(v.getId()));
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("exactMatch", TextPredicate.DOES_NOT_CONTAIN, "Test")
                .vertices();
        Assert.assertEquals(5, count(vertices));
        expectedVertexIds = Arrays.asList("v1", "v2", "v3", "v4", "v5");
        for (Vertex v : vertices) {
            assertTrue(expectedVertexIds.contains(v.getId()));
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("exactMatch", TextPredicate.DOES_NOT_CONTAIN, "Test Value")
                .vertices();
        Assert.assertEquals(5, count(vertices));
        expectedVertexIds = Arrays.asList("v1", "v2", "v3", "v4", "v5");
        for (Vertex v : vertices) {
            assertTrue(expectedVertexIds.contains(v.getId()));
        }

        graph.prepareVertex("v6", VISIBILITY_A)
                .setProperty("both", "susan-test", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v7", VISIBILITY_A)
                .setProperty("both", "susan-test", Visibility.EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("both", TextPredicate.DOES_NOT_CONTAIN, "susan")
                .vertices();
        Assert.assertEquals(5, count(vertices));
        expectedVertexIds = Arrays.asList("v1", "v2", "v3", "v4", "v5");
        for (Vertex v : vertices) {
            assertTrue(expectedVertexIds.contains(v.getId()));
        }
    }

    @Test
    public void testTextIndexStreamingPropertyValue() throws Exception {
        graph.defineProperty("none").dataType(String.class).textIndexHint(TextIndexHint.NONE).define();
        graph.defineProperty("both").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("fullText").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("none", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .setProperty("both", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .setProperty("fullText", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("both", TextPredicate.CONTAINS, "Test").vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("fullText", TextPredicate.CONTAINS, "Test").vertices()));
        Assert.assertEquals("un-indexed property shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("none", "Test").vertices()));
        Assert.assertEquals("un-indexed property shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("none", TextPredicate.CONTAINS, "Test").vertices()));
    }

    @Test
    public void testFieldBoost() throws Exception {
        if (!graph.isFieldBoostSupported()) {
            LOGGER.warn("Boost not supported");
            return;
        }

        graph.defineProperty("a")
                .dataType(String.class)
                .textIndexHint(TextIndexHint.ALL)
                .boost(1)
                .define();
        graph.defineProperty("b")
                .dataType(String.class)
                .textIndexHint(TextIndexHint.ALL)
                .boost(2)
                .define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("a", "Test Value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("b", "Test Value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        assertVertexIds(graph.query("Test", AUTHORIZATIONS_A).vertices(), new String[]{"v2", "v1"});
    }

    @Test
    public void testVertexBoost() throws Exception {
        if (!isEdgeBoostSupported()) {
            LOGGER.warn("Boost not supported");
            return;
        }

        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.addEdge("e1", v3, v2, "link", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1.prepareMutation().save(AUTHORIZATIONS_A_AND_B);
        v2.prepareMutation().save(AUTHORIZATIONS_A_AND_B);
        v3.prepareMutation().save(AUTHORIZATIONS_A_AND_B);

        assertVertexIds(graph.query(AUTHORIZATIONS_A).vertices(), new String[]{"v2", "v3", "v1"});
    }

    @Test
    public void testValueTypes() throws Exception {
        Date date = createDate(2014, 2, 24, 13, 0, 5);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("int", 5, VISIBILITY_A)
                .setProperty("bigInteger", BigInteger.valueOf(10), VISIBILITY_A)
                .setProperty("bigDecimal", BigDecimal.valueOf(1.1), VISIBILITY_A)
                .setProperty("double", 5.6, VISIBILITY_A)
                .setProperty("float", 6.4f, VISIBILITY_A)
                .setProperty("string", "test", VISIBILITY_A)
                .setProperty("byte", (byte) 5, VISIBILITY_A)
                .setProperty("long", (long) 5, VISIBILITY_A)
                .setProperty("boolean", true, VISIBILITY_A)
                .setProperty("geopoint", new GeoPoint(77, -33), VISIBILITY_A)
                .setProperty("short", (short) 5, VISIBILITY_A)
                .setProperty("date", date, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("int", 5).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("double", 5.6).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).range("float", 6.3f, 6.5f).vertices())); // can't search for 6.4f her because of float precision
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("string", "test").vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("byte", 5).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("long", 5).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("boolean", true).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("short", 5).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("date", date).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("bigInteger", BigInteger.valueOf(10)).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("bigInteger", 10).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("bigDecimal", BigDecimal.valueOf(1.1)).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("bigDecimal", 1.1).vertices()));
        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("geopoint", GeoCompare.WITHIN, new GeoCircle(77, -33, 1)).vertices()));
    }

    @Test
    public void testChangeVisibilityVertex() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);

        // change to same visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);
    }

    @Test
    public void testChangeVertexVisibilityAndAlterPropertyVisibilityAndChangePropertyAtTheSameTime() {
        Metadata metadata = new Metadata();
        metadata.add("m1", "m1-value1", VISIBILITY_EMPTY);
        metadata.add("m2", "m2-value1", VISIBILITY_EMPTY);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "age", 25, metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.createAuthorizations(AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_ALL);
        ExistingElementMutation<Vertex> m = v1.prepareMutation();
        m.alterElementVisibility(VISIBILITY_B);
        for (Property property : v1.getProperties()) {
            m.alterPropertyVisibility(property, VISIBILITY_B);
            m.setPropertyMetadata(property, "m1", "m1-value2", VISIBILITY_EMPTY);
        }
        m.save(AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(VISIBILITY_B, v1.getVisibility());
        List<Property> properties = toList(v1.getProperties());
        assertEquals(1, properties.size());
        assertEquals("age", properties.get(0).getName());
        assertEquals(VISIBILITY_B, properties.get(0).getVisibility());
        assertEquals(2, properties.get(0).getMetadata().entrySet().size());
        assertTrue(properties.get(0).getMetadata().containsKey("m1"));
        assertEquals("m1-value2", properties.get(0).getMetadata().getEntry("m1").getValue());
        assertEquals(VISIBILITY_EMPTY, properties.get(0).getMetadata().getEntry("m1").getVisibility());
        assertTrue(properties.get(0).getMetadata().containsKey("m2"));
        assertEquals("m2-value1", properties.get(0).getMetadata().getEntry("m2").getValue());
        assertEquals(VISIBILITY_EMPTY, properties.get(0).getMetadata().getEntry("m2").getVisibility());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull("v1 should not be returned for auth a", v1);

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_B)
                .has("age", Compare.EQUAL, 25)
                .vertices());
        assertEquals(1, vertices.size());

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices());
        assertEquals(0, vertices.size());
    }

    @Test
    public void testChangeVisibilityPropertiesWithPropertyKey() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("k1", "prop1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));

        assertEquals(1, count(graph.query(AUTHORIZATIONS_B).has("prop1", "value1").vertices()));
        assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("prop1", "value1").vertices()));

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value1"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value1"));
        }

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Property v1Prop1 = v1.getProperty("prop1");
        assertNotNull(v1Prop1);
        assertEquals(VISIBILITY_B, v1Prop1.getVisibility());

        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        graph.prepareEdge("e1", "v1", "v2", "label", VISIBILITY_EMPTY)
                .addPropertyValue("k2", "prop2", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterPropertyVisibility("k2","prop2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertNull(e1.getProperty("prop2"));

        assertEquals(1, count(graph.query(AUTHORIZATIONS_B).has("prop2", "value2").edges()));
        assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("prop2", "value2").edges()));

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop2", ElementType.EDGE, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value2"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop2", ElementType.EDGE, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value2"));
        }

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        Property e1prop1 = e1.getProperty("prop2");
        assertNotNull(e1prop1);
        assertEquals(VISIBILITY_B, e1prop1.getVisibility());
    }

    @Test
    public void testChangeVisibilityVertexProperties() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("prop1_key1", "value1", VISIBILITY_EMPTY);

        Metadata prop2Metadata = new Metadata();
        prop2Metadata.add("prop2_key1", "value1", VISIBILITY_EMPTY);

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .setProperty("prop2", "value2", prop2Metadata, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));
        assertNotNull(v1.getProperty("prop2"));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_B).has("prop1", "value1").vertices()));
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("prop1", "value1").vertices()));

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value1"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value1"));
        }

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Property v1Prop1 = v1.getProperty("prop1");
        assertNotNull(v1Prop1);
        Assert.assertEquals(1, toList(v1Prop1.getMetadata().entrySet()).size());
        assertEquals("value1", v1Prop1.getMetadata().getValue("prop1_key1"));
        assertNotNull(v1.getProperty("prop2"));

        // alter and set property in one mutation
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New", v1.getPropertyValue("prop1"));

        // alter visibility to the same visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New2", v1.getPropertyValue("prop1"));
    }

    @Test
    public void testAlterVisibilityAndSetMetadataInOneMutation() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("prop1_key1", "metadata1", VISIBILITY_EMPTY);

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .setPropertyMetadata("prop1", "prop1_key1", "metadata1New", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals(VISIBILITY_B, v1.getProperty("prop1").getVisibility());
        assertEquals("metadata1New", v1.getProperty("prop1").getMetadata().getValue("prop1_key1"));

        List<HistoricalPropertyValue> historicalPropertyValues = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        assertEquals(2, historicalPropertyValues.size());
        assertEquals("metadata1New", historicalPropertyValues.get(0).getMetadata().getValue("prop1_key1"));
        assertEquals("metadata1", historicalPropertyValues.get(1).getMetadata().getValue("prop1_key1"));
    }

    @Test
    public void testAlterPropertyVisibilityOverwritingProperty() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "prop1", "value1", VISIBILITY_EMPTY)
                .addPropertyValue("", "prop1", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        long beforeAlterTimestamp = IncreasingTime.currentTimeMillis();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyVisibility(v1.getProperty("", "prop1", VISIBILITY_A), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getProperties()));
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_EMPTY));
        assertEquals("value2", v1.getProperty("", "prop1", VISIBILITY_EMPTY).getValue());
        assertNull(v1.getProperty("", "prop1", VISIBILITY_A));

        v1 = graph.getVertex("v1", FetchHint.ALL, beforeAlterTimestamp, AUTHORIZATIONS_A);
        assertEquals(2, count(v1.getProperties()));
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_EMPTY));
        assertEquals("value1", v1.getProperty("", "prop1", VISIBILITY_EMPTY).getValue());
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_A));
        assertEquals("value2", v1.getProperty("", "prop1", VISIBILITY_A).getValue());
    }

    @Test
    public void testChangeVisibilityEdge() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);

        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareEdge("e1", v1, v2, "", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // test that we can see the edge with A and not B
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge
        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // test that we can see the edge with B and not A
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        Assert.assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge visibility to same
        e1 = graph.getEdge("e1", AUTHORIZATIONS_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);

        // test that we can see the edge with B and not A
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        Assert.assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
    }

    @Test
    public void testChangeVisibilityOnBadPropertyName() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_EMPTY)
                .setProperty("prop2", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        try {
            graph.getVertex("v1", AUTHORIZATIONS_A)
                    .prepareMutation()
                    .alterPropertyVisibility("propBad", VISIBILITY_B)
                    .save(AUTHORIZATIONS_A_AND_B);
            fail("show throw");
        } catch (VertexiumException ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testChangeVisibilityOnStreamingProperty() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility("propSmall", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility(largePropertyName, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        Assert.assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
    }

    @Test
    public void testChangePropertyMetadata() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("prop1_key1", "valueOld", VISIBILITY_EMPTY);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_EMPTY)
                .setProperty("prop2", "value2", null, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop1", "prop1_key1", "valueNew", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop2", "prop2_key1", "valueNew", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals("valueNew", v1.getProperty("prop2").getMetadata().getEntry("prop2_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("valueNew", v1.getProperty("prop2").getMetadata().getEntry("prop2_key1", VISIBILITY_EMPTY).getValue());
    }

    @Test
    public void testMutationChangePropertyVisibilityFollowedByMetadataUsingPropertyObject() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("prop1_key1", "valueOld", VISIBILITY_A);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Property p1 = v1.getProperty("prop1", VISIBILITY_A);
        v1.prepareMutation()
                .alterPropertyVisibility(p1, VISIBILITY_B)
                .setPropertyMetadata(p1, "prop1_key1", "valueNew", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals("valueNew", v1.getProperty("prop1", VISIBILITY_B).getMetadata().getEntry("prop1_key1", VISIBILITY_B).getValue());
    }

    @Test
    public void testMetadata() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ExistingElementMutation<Vertex> m = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).prepareMutation();
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", "metadata-value1aa", VISIBILITY_A);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", "metadata-value1ab", VISIBILITY_B);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_B), "metadata1", "metadata-value1bb", VISIBILITY_B);
        m.save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);

        Property prop1A = v1.getProperty("prop1", VISIBILITY_A);
        assertEquals(2, prop1A.getMetadata().entrySet().size());
        assertEquals("metadata-value1aa", prop1A.getMetadata().getValue("metadata1", VISIBILITY_A));
        assertEquals("metadata-value1ab", prop1A.getMetadata().getValue("metadata1", VISIBILITY_B));

        Property prop1B = v1.getProperty("prop1", VISIBILITY_B);
        assertEquals(1, prop1B.getMetadata().entrySet().size());
        assertEquals("metadata-value1bb", prop1B.getMetadata().getValue("metadata1", VISIBILITY_B));
    }

    @Test
    public void testIsVisibilityValid() {
        assertFalse(graph.isVisibilityValid(VISIBILITY_A, AUTHORIZATIONS_C));
        assertTrue(graph.isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_A_AND_B));
        assertTrue(graph.isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_B));
        assertTrue(graph.isVisibilityValid(VISIBILITY_EMPTY, AUTHORIZATIONS_A));
    }

    @Test
    public void testModifyVertexWithLowerAuthorizationThenOtherProperties() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.setProperty("prop1", "value1New", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop2", "value2")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});
    }

    @Test
    public void testPartialUpdateOfVertex() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1New", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop2", "value2")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});
    }

    @Test
    public void testPartialUpdateOfVertexPropertyKey() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop", "value1", VISIBILITY_A)
                .addPropertyValue("key2", "prop", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value2")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop", "value1New", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1New")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value2")
                .vertices();
        assertVertexIds(vertices, new String[]{"v1"});

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1")
                .vertices();
        assertVertexIds(vertices, new String[]{});
    }

    @Test
    public void testAddVertexWithoutIndexing() {
        if (isDefaultSearchIndex()) {
            return;
        }

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .vertices();
        assertVertexIds(vertices, new String[]{});
    }

    @Test
    public void testAlterVertexWithoutIndexing() {
        if (isDefaultSearchIndex()) {
            return;
        }

        graph.prepareVertex("v1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("prop1", "value1")
                .vertices();
        assertVertexIds(vertices, new String[]{});
    }

    @Test
    public void testAddEdgeWithoutIndexing() {
        if (isDefaultSearchIndex()) {
            return;
        }

        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .edges();
        assertEdgeIds(edges, new String[]{});
    }

    @Test
    public void testIteratorWithLessThanPageSizeResultsPageOne() {
        QueryParameters parameters = new QueryStringQueryParameters("*", AUTHORIZATIONS_EMPTY);
        parameters.setSkip(0);
        parameters.setLimit(5);
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(parameters, getVertices(3), false, false, false);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(3, count);
        assertNotNull("v was null", v);
        assertEquals("2", v.getId());
    }

    @Test
    public void testIteratorWithPageSizeResultsPageOne() {
        QueryParameters parameters = new QueryStringQueryParameters("*", AUTHORIZATIONS_EMPTY);
        parameters.setSkip(0);
        parameters.setLimit(5);
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(parameters, getVertices(5), false, false, false);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("4", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageOne() {
        QueryParameters parameters = new QueryStringQueryParameters("*", AUTHORIZATIONS_EMPTY);
        parameters.setSkip(0);
        parameters.setLimit(5);
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(parameters, getVertices(7), false, false, false);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("4", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageTwo() {
        QueryParameters parameters = new QueryStringQueryParameters("*", AUTHORIZATIONS_EMPTY);
        parameters.setSkip(5);
        parameters.setLimit(5);
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(parameters, getVertices(12), false, false, false);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(5, count);
        assertNotNull("v was null", v);
        assertEquals("9", v.getId());
    }

    @Test
    public void testIteratorWithMoreThanPageSizeResultsPageThree() {
        QueryParameters parameters = new QueryStringQueryParameters("*", AUTHORIZATIONS_EMPTY);
        parameters.setSkip(10);
        parameters.setLimit(5);
        DefaultGraphQueryIterable<Vertex> iterable = new DefaultGraphQueryIterable<>(parameters, getVertices(12), false, false, false);
        int count = 0;
        Iterator<Vertex> iterator = iterable.iterator();
        Vertex v = null;
        while (iterator.hasNext()) {
            count++;
            v = iterator.next();
            assertNotNull(v);
        }
        assertEquals(2, count);
        assertNotNull("v was null", v);
        assertEquals("11", v.getId());
    }

    @Test
    public void testGraphMetadata() {
        List<GraphMetadataEntry> existingMetadata = toList(graph.getMetadata());

        graph.setMetadata("test1", "value1old");
        graph.setMetadata("test1", "value1");
        graph.setMetadata("test2", "value2");

        assertEquals("value1", graph.getMetadata("test1"));
        assertEquals("value2", graph.getMetadata("test2"));
        assertEquals(null, graph.getMetadata("missingProp"));

        List<GraphMetadataEntry> newMetadata = toList(graph.getMetadata());
        assertEquals(existingMetadata.size() + 2, newMetadata.size());
    }

    @Test
    public void testSimilarityByText() {
        if (!graph.isQuerySimilarToTextSupported()) {
            LOGGER.warn("skipping test. Graph %s does not support query similar to text", graph.getClass().getName());
            return;
        }

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "Mary had a little lamb, His fleece was white as snow.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text", "Mary had a little tiger, His fleece was white as snow.", VISIBILITY_B)
                .save(AUTHORIZATIONS_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("text", "Mary had a little lamb.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v4", VISIBILITY_A)
                .setProperty("text", "His fleece was white as snow.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v5", VISIBILITY_A)
                .setProperty("text", "Mary had a little lamb, His fleece was black as snow.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v5", VISIBILITY_A)
                .setProperty("text", "Jack and Jill went up the hill to fetch a pail of water.", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<Vertex> vertices = toList(
                graph.querySimilarTo(new String[]{"text"}, "Mary had a little lamb, His fleece was white as snow", AUTHORIZATIONS_A_AND_B)
                        .minTermFrequency(1)
                        .maxQueryTerms(25)
                        .minDocFrequency(1)
                        .maxDocFrequency(10)
                        .percentTermsToMatch(0.5f)
                        .boost(2.0f)
                        .vertices()
        );
        assertTrue(vertices.size() > 0);

        vertices = toList(
                graph.querySimilarTo(new String[]{"text"}, "Mary had a little lamb, His fleece was white as snow", AUTHORIZATIONS_A)
                        .minTermFrequency(1)
                        .maxQueryTerms(25)
                        .minDocFrequency(1)
                        .maxDocFrequency(10)
                        .percentTermsToMatch(0.5f)
                        .boost(2.0f)
                        .vertices()
        );
        assertTrue(vertices.size() > 0);
    }

    @Test
    public void testAllPropertyHistoricalVersions() {
        Date time25 = createDate(2015, 4, 6, 16, 15, 0);
        Date time30 = createDate(2015, 4, 6, 16, 16, 0);

        Metadata metadata = new Metadata();
        metadata.add("author", "author1", VISIBILITY_A);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "age", 25, metadata, time25.getTime(), VISIBILITY_A)
                .addPropertyValue("k1", "name", "k1Time25Value", metadata, time25.getTime(), VISIBILITY_A)
                .addPropertyValue("k2", "name", "k2Time25Value", metadata, time25.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        metadata = new Metadata();
        metadata.add("author", "author2", VISIBILITY_A);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "age", 30, metadata, time30.getTime(), VISIBILITY_A)
                .addPropertyValue("k1", "name", "k1Time30Value", metadata, time30.getTime(), VISIBILITY_A)
                .addPropertyValue("k2", "name", "k2Time30Value", metadata, time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(6, values.size());

        for (int i = 0; i < 3; i++) {
            HistoricalPropertyValue item = values.get(i);
            assertEquals(time30, new Date(values.get(i).getTimestamp()));
            if (item.getPropertyName().equals("age")) {
                assertEquals(30, item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k1")) {
                assertEquals("k1Time30Value", item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k2")) {
                assertEquals("k2Time30Value", item.getValue());
            } else {
                fail("Invalid " + item);
            }
        }

        for (int i = 3; i < 6; i++) {
            HistoricalPropertyValue item = values.get(i);
            assertEquals(time25, new Date(values.get(i).getTimestamp()));
            if (item.getPropertyName().equals("age")) {
                assertEquals(25, item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k1")) {
                assertEquals("k1Time25Value", item.getValue());
            } else if (item.getPropertyName().equals("name") && item.getPropertyKey().equals("k2")) {
                assertEquals("k2Time25Value", item.getValue());
            } else {
                fail("Invalid " + item);
            }
        }
    }

    @Test
    public void testPropertyHistoricalVersions() {
        Date time25 = createDate(2015, 4, 6, 16, 15, 0);
        Date time30 = createDate(2015, 4, 6, 16, 16, 0);

        Metadata metadata = new Metadata();
        metadata.add("author", "author1", VISIBILITY_A);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "age", 25, metadata, time25.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        metadata = new Metadata();
        metadata.add("author", "author2", VISIBILITY_A);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "age", 30, metadata, time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues("", "age", VISIBILITY_A, AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        assertEquals(30, values.get(0).getValue());
        assertEquals(time30, new Date(values.get(0).getTimestamp()));
        assertEquals("author2", values.get(0).getMetadata().getValue("author", VISIBILITY_A));

        assertEquals(25, values.get(1).getValue());
        assertEquals(time25, new Date(values.get(1).getTimestamp()));
        assertEquals("author1", values.get(1).getMetadata().getValue("author", VISIBILITY_A));

        // make sure we get the correct age when we only ask for one value
        assertEquals(30, v1.getPropertyValue("", "age"));
        assertEquals("author2", v1.getProperty("", "age").getMetadata().getValue("author", VISIBILITY_A));
    }

    @Test
    public void testStreamingPropertyHistoricalVersions() {
        Date time25 = createDate(2015, 4, 6, 16, 15, 0);
        Date time30 = createDate(2015, 4, 6, 16, 16, 0);

        Metadata metadata = new Metadata();
        StreamingPropertyValue value1 = StreamingPropertyValue.create("value1");
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "text", value1, metadata, time25.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        StreamingPropertyValue value2 = StreamingPropertyValue.create("value2");
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "text", value2, metadata, time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues("", "text", VISIBILITY_A, AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        assertEquals("value2", ((StreamingPropertyValue) values.get(0).getValue()).readToString());
        assertEquals(time30, new Date(values.get(0).getTimestamp()));

        assertEquals("value1", ((StreamingPropertyValue) values.get(1).getValue()).readToString());
        assertEquals(time25, new Date(values.get(1).getTimestamp()));

        // make sure we get the correct age when we only ask for one value
        assertEquals("value2", ((StreamingPropertyValue) v1.getPropertyValue("", "text")).readToString());
    }

    @Test
    public void testGetVertexAtASpecificTimeInHistory() {
        Date time25 = createDate(2015, 4, 6, 16, 15, 0);
        Date time30 = createDate(2015, 4, 6, 16, 16, 0);

        Metadata metadata = new Metadata();
        Vertex v1 = graph.prepareVertex("v1", time25.getTime(), VISIBILITY_A)
                .addPropertyValue("", "age", 25, metadata, time25.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = graph.prepareVertex("v2", time25.getTime(), VISIBILITY_A)
                .addPropertyValue("", "age", 20, metadata, time25.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "label1", time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.prepareVertex("v1", time30.getTime(), VISIBILITY_A)
                .addPropertyValue("", "age", 30, metadata, time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", time30.getTime(), VISIBILITY_A)
                .addPropertyValue("", "age", 35, metadata, time30.getTime(), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        // verify current versions
        assertEquals(30, graph.getVertex("v1", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(20, graph.getVertex("v2", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(35, graph.getVertex("v3", AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        // verify old version
        assertEquals(25, graph.getVertex("v1", FetchHint.ALL, time25.getTime(), AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertNull("v3 should not exist at time25", graph.getVertex("v3", FetchHint.ALL, time25.getTime(), AUTHORIZATIONS_A));
        assertEquals("e1 should not exist", 0, count(graph.getEdges(FetchHint.ALL, time25.getTime(), AUTHORIZATIONS_A)));
    }

    @Test
    public void testSaveMultipleTimestampedValuesInSameMutationVertex() {
        String vertexId = "v1";
        String propertyKey = "k1";
        String propertyName = "p1";
        Map<String, Long> values = ImmutableMap.of(
                "value1", createDate(2016, 4, 6, 9, 20, 0).getTime(),
                "value2", createDate(2016, 5, 6, 9, 20, 0).getTime(),
                "value3", createDate(2016, 6, 6, 9, 20, 0).getTime(),
                "value4", createDate(2016, 7, 6, 9, 20, 0).getTime(),
                "value5", createDate(2016, 8, 6, 9, 20, 0).getTime()
        );

        ElementMutation<Vertex> vertexMutation = graph.prepareVertex(vertexId, VISIBILITY_EMPTY);
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            vertexMutation.addPropertyValue(propertyKey, propertyName, entry.getKey(), new Metadata(), entry.getValue(), VISIBILITY_EMPTY);
        }
        vertexMutation.save(AUTHORIZATIONS_EMPTY);
        graph.flush();

        Vertex retrievedVertex = graph.getVertex(vertexId, AUTHORIZATIONS_EMPTY);
        Iterable<HistoricalPropertyValue> historicalPropertyValues = retrievedVertex.getHistoricalPropertyValues(propertyKey, propertyName, VISIBILITY_EMPTY, null, null, AUTHORIZATIONS_EMPTY);
        compareHistoricalValues(values, historicalPropertyValues);
    }

    @Test
    public void testSaveMultipleTimestampedValuesInSameMutationEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);

        String edgeId = "e1";
        String propertyKey = "k1";
        String propertyName = "p1";
        Map<String, Long> values = ImmutableMap.of(
                "value1", createDate(2016, 4, 6, 9, 20, 0).getTime(),
                "value2", createDate(2016, 5, 6, 9, 20, 0).getTime(),
                "value3", createDate(2016, 6, 6, 9, 20, 0).getTime(),
                "value4", createDate(2016, 7, 6, 9, 20, 0).getTime(),
                "value5", createDate(2016, 8, 6, 9, 20, 0).getTime()
        );

        ElementMutation<Edge> edgeMutation = graph.prepareEdge(edgeId, v1, v2, "edgeLabel", VISIBILITY_EMPTY);
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            edgeMutation.addPropertyValue(propertyKey, propertyName, entry.getKey(), new Metadata(), entry.getValue(), VISIBILITY_EMPTY);
        }
        edgeMutation.save(AUTHORIZATIONS_EMPTY);
        graph.flush();

        Edge retrievedEdge = graph.getEdge(edgeId, AUTHORIZATIONS_EMPTY);
        Iterable<HistoricalPropertyValue> historicalPropertyValues = retrievedEdge.getHistoricalPropertyValues(propertyKey, propertyName, VISIBILITY_EMPTY, null, null, AUTHORIZATIONS_EMPTY);
        compareHistoricalValues(values, historicalPropertyValues);
    }

    private void compareHistoricalValues(Map<String, Long> expectedValues, Iterable<HistoricalPropertyValue> historicalPropertyValues) {
        Map<String, Long> expectedValuesCopy = new HashMap<>(expectedValues);
        for (HistoricalPropertyValue historicalPropertyValue : historicalPropertyValues) {
            String value = (String) historicalPropertyValue.getValue();
            if (!expectedValuesCopy.containsKey(value)) {
                throw new VertexiumException("Expected historical values to contain: " + value);
            }
            long expectedValue = expectedValuesCopy.remove(value);
            long ts = historicalPropertyValue.getTimestamp();
            assertEquals(expectedValue, ts);
        }
        if (expectedValuesCopy.size() > 0) {
            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, Long> entry : expectedValuesCopy.entrySet()) {
                result.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
            }
            throw new VertexiumException("Missing historical values:\n" + result.toString());
        }
    }

    @Test
    public void testGraphQueryWithTermsAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.defineProperty("emptyField").dataType(Integer.class).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", "Joseph", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", "Joseph", VISIBILITY_B)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", "label1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", "v1", "v2", "label1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", "v1", "v2", "label2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("name", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        if (vertexPropertyCountByValue == null) {
            return;
        }
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("Joseph"));

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("emptyField", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        if (vertexPropertyCountByValue == null) {
            return;
        }
        assertEquals(0, vertexPropertyCountByValue.size());

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("name", ElementType.VERTEX, AUTHORIZATIONS_A_AND_B);
        if (vertexPropertyCountByValue == null) {
            return;
        }
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joseph"));

        Map<Object, Long> edgePropertyCountByValue = queryGraphQueryWithTermsAggregation(Edge.LABEL_PROPERTY_NAME, ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        if (edgePropertyCountByValue == null) {
            return;
        }
        assertEquals(2, edgePropertyCountByValue.size());
        assertEquals(2L, (long) edgePropertyCountByValue.get("label1"));
        assertEquals(1L, (long) edgePropertyCountByValue.get("label2"));
    }

    private boolean isSearchIndexFieldLevelSecuritySupported() {
        if (graph instanceof GraphWithSearchIndex) {
            return ((GraphWithSearchIndex) graph).getSearchIndex().isFieldLevelSecuritySupported();
        }
        return true;
    }

    @Test
    public void testGraphQueryVertexWithTermsAggregationAlterElementVisibility() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_A_AND_B);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_A);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(1, propertyCountByValue.size());
    }

    @Test
    public void testGraphQueryEdgeWithTermsAggregationAlterElementVisibility() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", "edge", VISIBILITY_A)
                .addPropertyValue("k1", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_B);
        if (propertyCountByValue == null) {
            return;
        }
        assertEquals(1, propertyCountByValue.size());
    }

    private Map<Object, Long> queryGraphQueryWithTermsAggregation(String propertyName, ElementType elementType, Authorizations authorizations) {
        Query q = graph.query(authorizations).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", propertyName);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", agg.getClass().getName());
            return null;
        }
        q.addAggregation(agg);
        TermsResult aggregationResult = (elementType == ElementType.VERTEX ? q.vertices() : q.edges()).getAggregationResult("terms-count", TermsResult.class);
        return termsBucketToMap(aggregationResult.getBuckets());
    }

    @Test
    public void testGraphQueryWithNestedTermsAggregation() {
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        graph.defineProperty("gender").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "male", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "male", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "female", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "female", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = queryGraphQueryWithNestedTermsAggregation("name", "gender", AUTHORIZATIONS_A_AND_B);
        if (vertexPropertyCountByValue == null) {
            return;
        }
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    private Map<Object, Map<Object, Long>> queryGraphQueryWithNestedTermsAggregation(String propertyNameFirst, String propertyNameSecond, Authorizations authorizations) {
        Query q = graph.query(authorizations).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", propertyNameFirst);
        agg.addNestedAggregation(new TermsAggregation("nested", propertyNameSecond));
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", agg.getClass().getName());
            return null;
        }
        q.addAggregation(agg);
        TermsResult aggregationResult = q.vertices().getAggregationResult("terms-count", TermsResult.class);
        return nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");
    }

    @Test
    public void testGraphQueryWithHistogramAggregation() throws ParseException {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        graph.defineProperty("emptyField").dataType(Integer.class).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 25, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1990-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-08-15"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 20, VISIBILITY_A)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-03-02"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> histogram = queryGraphQueryWithHistogramAggregation("age", "1", 0L, new HistogramAggregation.ExtendedBounds<>(20L, 25L), AUTHORIZATIONS_EMPTY);
        if (histogram == null) {
            return;
        }
        assertEquals(6, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("20"));

        histogram = queryGraphQueryWithHistogramAggregation("age", "1", null, null, AUTHORIZATIONS_A_AND_B);
        if (histogram == null) {
            return;
        }
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(3L, (long) histogram.get("20"));

        // field that doesn't have any values
        histogram = queryGraphQueryWithHistogramAggregation("emptyField", "1", null, null, AUTHORIZATIONS_A_AND_B);
        if (histogram == null) {
            return;
        }
        assertEquals(0, histogram.size());

        // date by 'year'
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", "year", null, null, AUTHORIZATIONS_EMPTY);
        if (histogram == null) {
            return;
        }
        assertEquals(2, histogram.size());

        // date by milliseconds
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", (365L * 24L * 60L * 60L * 1000L) + "", null, null, AUTHORIZATIONS_EMPTY);
        if (histogram == null) {
            return;
        }
        assertEquals(2, histogram.size());
    }

    private Map<Object, Long> queryGraphQueryWithHistogramAggregation(
            String propertyName,
            String interval,
            Long minDocCount,
            HistogramAggregation.ExtendedBounds extendedBounds,
            Authorizations authorizations
    ) {
        Query q = graph.query(authorizations).limit(0);
        HistogramAggregation agg = new HistogramAggregation("hist-count", propertyName, interval, minDocCount);
        agg.setExtendedBounds(extendedBounds);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", HistogramAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return histogramBucketToMap(q.vertices().getAggregationResult("hist-count", HistogramResult.class).getBuckets());
    }

    @Test
    public void testGraphQueryWithStatisticsAggregation() throws ParseException {
        graph.defineProperty("emptyField").dataType(Integer.class).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 30, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        StatisticsResult stats = queryGraphQueryWithStatisticsAggregation("age", AUTHORIZATIONS_EMPTY);
        if (stats == null) {
            return;
        }
        assertEquals(3, stats.getCount());
        assertEquals(65.0, stats.getSum(), 0.1);
        assertEquals(20.0, stats.getMin(), 0.1);
        assertEquals(25.0, stats.getMax(), 0.1);
        assertEquals(2.35702, stats.getStandardDeviation(), 0.1);
        assertEquals(21.666666, stats.getAverage(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("emptyField", AUTHORIZATIONS_EMPTY);
        if (stats == null) {
            return;
        }
        assertEquals(0, stats.getCount());
        assertEquals(0.0, stats.getSum(), 0.1);
        assertEquals(0.0, stats.getMin(), 0.1);
        assertEquals(0.0, stats.getMax(), 0.1);
        assertEquals(0.0, stats.getAverage(), 0.1);
        assertEquals(0.0, stats.getStandardDeviation(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("age", AUTHORIZATIONS_A_AND_B);
        if (stats == null) {
            return;
        }
        assertEquals(4, stats.getCount());
        assertEquals(95.0, stats.getSum(), 0.1);
        assertEquals(20.0, stats.getMin(), 0.1);
        assertEquals(30.0, stats.getMax(), 0.1);
        assertEquals(23.75, stats.getAverage(), 0.1);
        assertEquals(4.14578, stats.getStandardDeviation(), 0.1);
    }

    private StatisticsResult queryGraphQueryWithStatisticsAggregation(String propertyName, Authorizations authorizations) {
        Query q = graph.query(authorizations).limit(0);
        StatisticsAggregation agg = new StatisticsAggregation("stats", propertyName);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", StatisticsAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("stats", StatisticsResult.class);
    }

    @Test
    public void testGraphQueryWithGeohashAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();

        graph.defineProperty("emptyField").dataType(GeoPoint.class).define();
        graph.defineProperty("location").dataType(GeoPoint.class).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "location", new GeoPoint(50, -10, "pt1"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", "location", new GeoPoint(39, -77, "pt2"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", "location", new GeoPoint(39.1, -77.1, "pt3"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", "location", new GeoPoint(39.2, -77.2, "pt4"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<String, Long> histogram = queryGraphQueryWithGeohashAggregation("location", 2, AUTHORIZATIONS_EMPTY);
        if (histogram == null) {
            return;
        }
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("dq"));

        histogram = queryGraphQueryWithGeohashAggregation("emptyField", 2, AUTHORIZATIONS_EMPTY);
        if (histogram == null) {
            return;
        }
        assertEquals(0, histogram.size());

        histogram = queryGraphQueryWithGeohashAggregation("location", 2, AUTHORIZATIONS_A_AND_B);
        if (histogram == null) {
            return;
        }
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(3L, (long) histogram.get("dq"));
    }

    @Test
    public void testGraphQueryWithCalendarFieldAggregation() {
        graph.prepareVertex("v0", VISIBILITY_EMPTY)
                .addPropertyValue("", "other_field", createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2017, Calendar.MAY, 26, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v3", VISIBILITY_A_AND_B)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 27, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v4", VISIBILITY_A_AND_B)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 24, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v5", VISIBILITY_A_AND_B)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 25, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v6", VISIBILITY_A_AND_B)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 30, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        // hour of day
        QueryResultsIterable<Vertex> results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.HOUR_OF_DAY))
                .limit(0)
                .vertices();

        HistogramResult aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(2, aggResult.getBucketByKey(10).getCount());
        assertEquals(4, aggResult.getBucketByKey(12).getCount());

        // day of week
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.DAY_OF_WEEK))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(5, count(aggResult.getBuckets()));
        assertEquals(1, aggResult.getBucketByKey(Calendar.SUNDAY).getCount());
        assertEquals(1, aggResult.getBucketByKey(Calendar.MONDAY).getCount());
        assertEquals(2, aggResult.getBucketByKey(Calendar.WEDNESDAY).getCount());
        assertEquals(1, aggResult.getBucketByKey(Calendar.FRIDAY).getCount());
        assertEquals(1, aggResult.getBucketByKey(Calendar.SATURDAY).getCount());

        // day of month
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.DAY_OF_MONTH))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(5, count(aggResult.getBuckets()));
        assertEquals(1, aggResult.getBucketByKey(24).getCount());
        assertEquals(1, aggResult.getBucketByKey(25).getCount());
        assertEquals(1, aggResult.getBucketByKey(26).getCount());
        assertEquals(2, aggResult.getBucketByKey(27).getCount());
        assertEquals(1, aggResult.getBucketByKey(30).getCount());

        // month
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.MONTH))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(Calendar.APRIL).getCount());
        assertEquals(1, aggResult.getBucketByKey(Calendar.MAY).getCount());

        // year
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.YEAR))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(2016).getCount());
        assertEquals(1, aggResult.getBucketByKey(2017).getCount());

        // week of year
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.WEEK_OF_YEAR))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(18).getCount());
        assertEquals(1, aggResult.getBucketByKey(21).getCount());
    }

    @Test
    public void testGraphQueryWithCalendarFieldAggregationNested() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 27, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", "date", createDate(2016, Calendar.APRIL, 28, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        CalendarFieldAggregation agg = new CalendarFieldAggregation("agg1", "date", null, TimeZone.getDefault(), Calendar.DAY_OF_WEEK);
        agg.addNestedAggregation(new CalendarFieldAggregation("aggNested", "date", null, TimeZone.getDefault(), Calendar.HOUR_OF_DAY));
        QueryResultsIterable<Vertex> results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(agg)
                .limit(0)
                .vertices();

        HistogramResult aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);

        HistogramBucket bucket = aggResult.getBucketByKey(Calendar.WEDNESDAY);
        assertEquals(3, bucket.getCount());
        HistogramResult nestedResult = (HistogramResult) bucket.getNestedResults().get("aggNested");
        assertEquals(2, nestedResult.getBucketByKey(10).getCount());
        assertEquals(1, nestedResult.getBucketByKey(12).getCount());

        bucket = aggResult.getBucketByKey(Calendar.THURSDAY);
        assertEquals(1, bucket.getCount());
        nestedResult = (HistogramResult) bucket.getNestedResults().get("aggNested");
        assertEquals(1, nestedResult.getBucketByKey(10).getCount());
    }

    @Test
    public void testLargeFieldValuesThatAreMarkedWithExactMatch() {
        graph.defineProperty("field1").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeText.append("test ");
        }

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", "field1", largeText.toString(), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_EMPTY);
        graph.flush();
    }

    private Map<String, Long> queryGraphQueryWithGeohashAggregation(String propertyName, int precision, Authorizations authorizations) {
        Query q = graph.query(authorizations).limit(0);
        GeohashAggregation agg = new GeohashAggregation("geo-count", propertyName, precision);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", GeohashAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return geoHashBucketToMap(q.vertices().getAggregationResult("geo-count", GeohashResult.class).getBuckets());
    }

    @Test
    public void testGetVertexPropertyCountByValue() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", "Joseph", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k2", "name", "Joseph", VISIBILITY_B)
                .addPropertyValue("", "age", 20, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> vertexPropertyCountByValue = graph.getVertexPropertyCountByValue("name", AUTHORIZATIONS_EMPTY);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("joseph"));

        vertexPropertyCountByValue = graph.getVertexPropertyCountByValue("name", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joe"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("joseph"));
    }

    @Test
    public void testGetCounts() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "edge1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        assertEquals(2, graph.getVertexCount(AUTHORIZATIONS_A));
        assertEquals(1, graph.getEdgeCount(AUTHORIZATIONS_A));
    }

    @Test
    public void testFetchHintsEdgeLabels() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        graph.addEdge("e v1->v2", v1, v2, "labelA", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("e v1->v3", v1, v3, "labelB", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHint.EDGE_LABELS, AUTHORIZATIONS_ALL);
        List<String> edgeLabels = toList(v1.getEdgeLabels(Direction.BOTH, AUTHORIZATIONS_ALL));
        assertEquals(2, edgeLabels.size());
        assertTrue("labelA missing", edgeLabels.contains("labelA"));
        assertTrue("labelB missing", edgeLabels.contains("labelB"));
    }

    @Test
    public void testIPAddress() {
        graph.defineProperty("ipAddress2").dataType(IpV4Address.class).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "ipAddress1", new IpV4Address("192.168.0.1"), VISIBILITY_A)
                .addPropertyValue("k1", "ipAddress2", new IpV4Address("192.168.0.2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .addPropertyValue("k1", "ipAddress1", new IpV4Address("192.168.0.5"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .addPropertyValue("k1", "ipAddress1", new IpV4Address("192.168.1.1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(new IpV4Address("192.168.0.1"), v1.getPropertyValue("ipAddress1"));
        assertEquals(new IpV4Address(192, 168, 0, 2), v1.getPropertyValue("ipAddress2"));

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A).has("ipAddress1", Compare.EQUAL, new IpV4Address("192.168.0.1")).vertices());
        assertEquals(1, vertices.size());
        assertEquals("v1", vertices.get(0).getId());

        vertices = sortById(toList(
                graph.query(AUTHORIZATIONS_A)
                        .range("ipAddress1", new IpV4Address("192.168.0.0"), new IpV4Address("192.168.0.255"))
                        .vertices()
        ));
        assertEquals(2, vertices.size());
        assertEquals("v1", vertices.get(0).getId());
        assertEquals("v2", vertices.get(1).getId());
    }

    @Test
    public void testVertexHashCodeAndEquals() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1Loaded = graph.getVertex("v1", AUTHORIZATIONS_A);

        assertEquals(v1Loaded.hashCode(), v1.hashCode());
        assertTrue(v1Loaded.equals(v1));

        assertNotEquals(v1Loaded.hashCode(), v2.hashCode());
        assertFalse(v1Loaded.equals(v2));
    }

    @Test
    public void testEdgeHashCodeAndEquals() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        Edge e1 = graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge e2 = graph.prepareEdge("e2", v1, v2, "label1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Edge e1Loaded = graph.getEdge("e1", AUTHORIZATIONS_A);

        assertEquals(e1Loaded.hashCode(), e1.hashCode());
        assertTrue(e1Loaded.equals(e1));

        assertNotEquals(e1Loaded.hashCode(), e2.hashCode());
        assertFalse(e1Loaded.equals(e2));
    }

    private List<Vertex> getVertices(long count) {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Vertex vertex = graph.addVertex(Integer.toString(i), VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
            vertices.add(vertex);
        }
        return vertices;
    }

    private boolean isDefaultSearchIndex() {
        if (!(graph instanceof GraphWithSearchIndex)) {
            return false;
        }

        GraphWithSearchIndex graphWithSearchIndex = (GraphWithSearchIndex) graph;
        return graphWithSearchIndex.getSearchIndex() instanceof DefaultSearchIndex;
    }

    protected List<Vertex> sortById(List<Vertex> vertices) {
        Collections.sort(vertices, new Comparator<Vertex>() {
            @Override
            public int compare(Vertex o1, Vertex o2) {
                return o1.getId().compareTo(o2.getId());
            }
        });
        return vertices;
    }

    protected void assertVertexIds(Iterable<Vertex> vertices, String[] expectedIds) {
        String verticesIdsString = idsToString(vertices);
        String expectedIdsString = idsToString(expectedIds);
        List<Vertex> verticesList = toList(vertices);
        assertEquals("ids length mismatch found:[" + verticesIdsString + "] expected:[" + expectedIdsString + "]", expectedIds.length, verticesList.size());
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals("at offset: " + i + " found:[" + verticesIdsString + "] expected:[" + expectedIdsString + "]", expectedIds[i], verticesList.get(i).getId());
        }
    }

    private String idsToString(String[] ids) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String id : ids) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(id);
            first = false;
        }
        return sb.toString();
    }

    private String idsToString(Iterable<Vertex> vertices) {
        List<String> idsList = toList(new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        });
        String[] idsArray = idsList.toArray(new String[idsList.size()]);
        return idsToString(idsArray);
    }

    private void assertEvents(GraphEvent... expectedEvents) {
        assertEquals("Different number of events occurred than were asserted", expectedEvents.length, graphEvents.size());

        for (int i = 0; i < expectedEvents.length; i++) {
            assertEquals(expectedEvents[i], graphEvents.get(i));
        }
    }

    protected void assertEdgeIds(Iterable<Edge> edges, String[] ids) {
        List<Edge> edgesList = toList(edges);
        assertEquals("ids length mismatch", ids.length, edgesList.size());
        for (int i = 0; i < ids.length; i++) {
            assertEquals("at offset: " + i, ids[i], edgesList.get(i).getId());
        }
    }

    protected void assertResultsCount(int expectedCountAndTotalHits, QueryResultsIterable<? extends Element> results) {
        assertEquals(expectedCountAndTotalHits, results.getTotalHits());
        assertEquals(expectedCountAndTotalHits, count(results));
    }

    protected void assertResultsCount(
            int expectedCount, int expectedTotalHits, QueryResultsIterable<? extends Element> results
    ) {
        assertEquals(expectedTotalHits, results.getTotalHits());
        assertEquals(expectedCount, count(results));
    }

    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        return false;
    }

    protected boolean disableEdgeIndexing(Graph graph) {
        try {
            if (!(graph instanceof GraphWithSearchIndex)) {
                LOGGER.debug("Graph does not have a search index");
                return false;
            }

            SearchIndex searchIndex = ((GraphWithSearchIndex) graph).getSearchIndex();

            Field configField = findPrivateField(searchIndex.getClass(), "config");
            if (configField == null) {
                LOGGER.debug("Could not find 'config' field");
                return false;
            }

            configField.setAccessible(true);

            Object config = configField.get(searchIndex);
            if (config == null) {
                LOGGER.debug("Could not get 'config' field");
                return false;
            }

            Field indexEdgesField = findPrivateField(config.getClass(), "indexEdges");
            if (indexEdgesField == null) {
                LOGGER.debug("Could not find 'indexEdgesField' field");
                return false;
            }

            indexEdgesField.setAccessible(true);

            indexEdgesField.set(config, false);

            return true;
        } catch (Exception ex) {
            throw new VertexiumException("Could not disableEdgeIndexing", ex);
        }
    }

    private Field findPrivateField(Class clazz, String name) {
        try {
            return clazz.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
            if (clazz.getSuperclass() != null) {
                return findPrivateField(clazz.getSuperclass(), name);
            }
            return null;
        }
    }

    private Map<Object, Long> termsBucketToMap(Iterable<TermsBucket> buckets) {
        Map<Object, Long> results = new HashMap<>();
        for (TermsBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    private Map<Object, Map<Object, Long>> nestedTermsBucketToMap(Iterable<TermsBucket> buckets, String nestedAggName) {
        Map<Object, Map<Object, Long>> results = new HashMap<>();
        for (TermsBucket entry : buckets) {
            TermsResult nestedResults = (TermsResult) entry.getNestedResults().get(nestedAggName);
            if (nestedResults == null) {
                throw new VertexiumException("Could not find nested: " + nestedAggName);
            }
            results.put(entry.getKey(), termsBucketToMap(nestedResults.getBuckets()));
        }
        return results;
    }

    private Map<Object, Long> histogramBucketToMap(Iterable<HistogramBucket> buckets) {
        Map<Object, Long> results = new HashMap<>();
        for (HistogramBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    private Map<String, Long> geoHashBucketToMap(Iterable<GeohashBucket> buckets) {
        Map<String, Long> results = new HashMap<>();
        for (GeohashBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    protected abstract boolean isEdgeBoostSupported();

    // Historical Property Value tests
    @Test
    public void historicalPropertyValueAddProp() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Add property
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);

        assertEquals(3, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertEquals("prop3_A", values.get(2).getPropertyName());
    }

    @Test
    public void historicalPropertyValueDeleteProp() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // remove property
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .softDeleteProperties("prop2_B")
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();


        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(4, values.size());

        boolean isDeletedExpected = false;
        for (int i = 0; i < 4; i++) {
            HistoricalPropertyValue item = values.get(i);
            if (item.getPropertyName().equals("prop1_A")) {
                assertEquals("prop1_A", values.get(i).getPropertyName());
                assertEquals(false, values.get(i).isDeleted());
            } else if (item.getPropertyName().equals("prop2_B")) {
                assertEquals("prop2_B", values.get(i).getPropertyName());
                assertEquals(isDeletedExpected, values.get(i).isDeleted());
                isDeletedExpected = !isDeletedExpected;
            } else if (item.getPropertyName().equals("prop3_A")) {
                assertEquals("prop3_A", values.get(i).getPropertyName());
                assertEquals(false, values.get(i).isDeleted());
            } else {
                fail("Invalid " + item);
            }
        }
    }

    @Test
    public void testPropertyHistoricalVersionsAfterVertexPropertyDeleteByName() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("", "age", 25, VISIBILITY_A)
                .addPropertyValue("", "gender", "F", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        v1.deleteProperties("age", AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(1, values.size());

        v1.prepareMutation()
                .alterPropertyVisibility("gender", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        v1.deleteProperties("gender", AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(0, values.size());
    }

    @Test
    public void testPropertyHistoricalVersionsAfterEdgePropertyDeleteByName() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        graph.prepareEdge("e1", "v1", "v2", "label", VISIBILITY_A)
                .addPropertyValue("k1", "test1", "value", VISIBILITY_A)
                .addPropertyValue("k2", "test2", "value", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        e1.deleteProperties("test1", AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(1, values.size());

        e1.prepareMutation()
                .alterPropertyVisibility("k2", "test2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(2, values.size());

        e1.deleteProperties("test2", AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(0, values.size());
    }

    @Test
    public void testPropertyHistoricalVersionsAfterVertexPropertyDeleteByKeyName() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "age", 25, VISIBILITY_A)
                .addPropertyValue("k2", "age", 25, VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "F", VISIBILITY_A)
                .addPropertyValue("k2", "gender", "F", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(4, values.size());

        v1.deleteProperty("k1", "age", AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(3, values.size());

        v1.prepareMutation()
                .alterPropertyVisibility("k1", "gender", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(4, values.size());

        v1.deleteProperty("k1", "gender", AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        Collections.reverse(values);
        assertEquals(2, values.size());
        assertEquals("k2", values.get(0).getPropertyKey());
        assertEquals("gender", values.get(0).getPropertyName());
        assertEquals(false, values.get(0).isDeleted());
        assertEquals("k2", values.get(1).getPropertyKey());
        assertEquals("age", values.get(1).getPropertyName());
        assertEquals(false, values.get(1).isDeleted());
    }

    @Test
    public void testPropertyHistoricalVersionsAfterEdgePropertyDeleteByKeyAndName() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        graph.prepareEdge("e1", "v1", "v2", "label", VISIBILITY_A)
                .addPropertyValue("k1", "test1", "value", VISIBILITY_A)
                .addPropertyValue("k2", "test1", "value", VISIBILITY_A)
                .addPropertyValue("k1", "test2", "value", VISIBILITY_A)
                .addPropertyValue("k2", "test2", "value", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        List<HistoricalPropertyValue> values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(4, values.size());

        e1.deleteProperty("k1", "test1", AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(3, values.size());

        e1.prepareMutation()
                .alterPropertyVisibility("k1", "test2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(4, values.size());

        e1.deleteProperty("k1", "test2", AUTHORIZATIONS_A);
        graph.flush();

        e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        values = toList(e1.getHistoricalPropertyValues(AUTHORIZATIONS_A));
        assertEquals(2, values.size());
        assertEquals("k2", values.get(0).getPropertyKey());
        assertEquals("test2", values.get(0).getPropertyName());
        assertEquals(false, values.get(0).isDeleted());
        assertEquals("k2", values.get(1).getPropertyKey());
        assertEquals("test1", values.get(1).getPropertyName());
        assertEquals(false, values.get(1).isDeleted());
    }

    @Test
    public void historicalPropertyValueModifyPropValue() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // modify property value
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", "value4", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Restore
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(5, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertEquals(false, values.get(0).isDeleted());
        assertEquals("value1", values.get(0).getValue());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertEquals(false, values.get(1).isDeleted());
        assertEquals("value2", values.get(1).getValue());
        assertEquals("prop3_A", values.get(2).getPropertyName());
        assertEquals(false, values.get(2).isDeleted());
        assertEquals("value3", values.get(2).getValue());
        assertEquals("prop3_A", values.get(3).getPropertyName());
        assertEquals(false, values.get(3).isDeleted());
        assertEquals("value4", values.get(3).getValue());
        assertEquals("prop3_A", values.get(4).getPropertyName());
        assertEquals(false, values.get(4).isDeleted());
        assertEquals("value3", values.get(4).getValue());
    }

    @Test
    public void historicalPropertyValueModifyPropVisibility() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // modify property value
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1_A", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Restore
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1_A", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<HistoricalPropertyValue> values = toList(v1.getHistoricalPropertyValues(AUTHORIZATIONS_A_AND_B));
        Collections.reverse(values);
        assertEquals(5, values.size());
        assertEquals("prop1_A", values.get(0).getPropertyName());
        assertEquals(false, values.get(0).isDeleted());
        assertEquals(VISIBILITY_A, values.get(0).getPropertyVisibility());
        assertEquals("prop2_B", values.get(1).getPropertyName());
        assertEquals(false, values.get(1).isDeleted());
        assertEquals(VISIBILITY_B, values.get(1).getPropertyVisibility());
        assertEquals("prop3_A", values.get(2).getPropertyName());
        assertEquals(false, values.get(2).isDeleted());
        assertEquals(VISIBILITY_A, values.get(2).getPropertyVisibility());
        assertEquals("prop1_A", values.get(3).getPropertyName());
        assertEquals(false, values.get(3).isDeleted());
        assertEquals(VISIBILITY_B, values.get(3).getPropertyVisibility());
        assertEquals("prop1_A", values.get(4).getPropertyName());
        assertEquals(false, values.get(4).isDeleted());
        assertEquals(VISIBILITY_A, values.get(4).getPropertyVisibility());
    }
}
