package org.vertexium.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
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
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.vertexium.test.util.VertexiumAssert.*;
import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.StreamUtils.stream;

@RunWith(JUnit4.class)
public abstract class GraphTestBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphTestBase.class);
    public static final String VISIBILITY_A_STRING = "a";
    public static final String VISIBILITY_B_STRING = "b";
    public static final String VISIBILITY_C_STRING = "c";
    public static final String VISIBILITY_MIXED_CASE_STRING = "MIXED_CASE_a";
    public static final Visibility VISIBILITY_A = new Visibility(VISIBILITY_A_STRING);
    public static final Visibility VISIBILITY_A_AND_B = new Visibility("a&b");
    public static final Visibility VISIBILITY_B = new Visibility(VISIBILITY_B_STRING);
    public static final Visibility VISIBILITY_C = new Visibility(VISIBILITY_C_STRING);
    public static final Visibility VISIBILITY_MIXED_CASE_a = new Visibility("((MIXED_CASE_a))|b");
    public static final Visibility VISIBILITY_EMPTY = new Visibility("");
    public static final String LABEL_LABEL1 = "label1";
    public static final String LABEL_LABEL2 = "label2";
    public static final String LABEL_LABEL3 = "label3";
    public static final String LABEL_BAD = "bad";
    public final Authorizations AUTHORIZATIONS_A;
    public final Authorizations AUTHORIZATIONS_B;
    public final Authorizations AUTHORIZATIONS_C;
    public final Authorizations AUTHORIZATIONS_MIXED_CASE_a_AND_B;
    public final Authorizations AUTHORIZATIONS_A_AND_B;
    public final Authorizations AUTHORIZATIONS_B_AND_C;
    public final Authorizations AUTHORIZATIONS_EMPTY;
    public final Authorizations AUTHORIZATIONS_BAD;
    public final Authorizations AUTHORIZATIONS_ALL;
    public static final int LARGE_PROPERTY_VALUE_SIZE = 1024 * 1024 + 1;

    protected Graph graph;

    protected abstract Graph createGraph() throws Exception;

    public Graph getGraph() {
        return graph;
    }

    public GraphTestBase() {
        AUTHORIZATIONS_A = createAuthorizations("a");
        AUTHORIZATIONS_B = createAuthorizations("b");
        AUTHORIZATIONS_C = createAuthorizations("c");
        AUTHORIZATIONS_A_AND_B = createAuthorizations("a", "b");
        AUTHORIZATIONS_B_AND_C = createAuthorizations("b", "c");
        AUTHORIZATIONS_MIXED_CASE_a_AND_B = createAuthorizations("MIXED_CASE_a", "b");
        AUTHORIZATIONS_EMPTY = createAuthorizations();
        AUTHORIZATIONS_BAD = createAuthorizations("bad");
        AUTHORIZATIONS_ALL = createAuthorizations("a", "b", "c", "MIXED_CASE_a");
    }

    protected abstract Authorizations createAuthorizations(String... auths);

    protected abstract void addAuthorizations(String... authorizations);

    @Before
    public void before() throws Exception {
        graph = createGraph();
        clearGraphEvents();
        graph.addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                addGraphEvent(graphEvent);
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

    // Need this to given occasional output so Travis doesn't fail the build for no output
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

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
        PropertyValue propSmall = StreamingPropertyValue.create(new ByteArrayInputStream("value1".getBytes()), String.class, 6L);
        PropertyValue propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedLargeValue.getBytes()),
                String.class,
                null
        );
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Object> propSmallValues = v1.getPropertyValues("propSmall");
        Assert.assertEquals(1, count(propSmallValues));
        Object propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        StreamingPropertyValue value = (StreamingPropertyValue) propSmallValue;
        assertEquals(String.class, value.getValueType());
        assertEquals("value1".getBytes().length, (long) value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        Iterable<Object> propLargeValues = v1.getPropertyValues(largePropertyName);
        Assert.assertEquals(1, count(propLargeValues));
        Object propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, (long) value.getLength());
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
        assertEquals("value1".getBytes().length, (long) value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        propLargeValues = v1.getPropertyValues(largePropertyName);
        Assert.assertEquals(1, count(propLargeValues));
        propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, (long) value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
    }

    @Test
    public void testStreamingPropertyValueLargeReads() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        PropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");
        byte[] buffer = new byte[LARGE_PROPERTY_VALUE_SIZE * 2];
        int leftToRead = expectedLargeValueBytes.length;
        InputStream in = prop.getInputStream();
        for (int expectedOffset = 0; expectedOffset < expectedLargeValueBytes.length; ) {
            int sizeRead = in.read(buffer);
            for (int j = 0; j < sizeRead; j++, expectedOffset++, leftToRead--) {
                assertEquals("invalid data at offset " + expectedOffset, expectedLargeValueBytes[expectedOffset], buffer[j]);
            }
        }
        assertEquals(0, leftToRead);
        assertEquals(-1, in.read(buffer));
    }

    @Test
    public void testStreamingPropertyDecreasingSize() throws IOException {
        Metadata metadata = new Metadata();
        Long timestamp = System.currentTimeMillis();
        String expectedValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedValue.getBytes()),
                String.class,
                (long) expectedValue.length()
        );
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "largeProp", propLarge, metadata, timestamp, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue("key1", "largeProp");
        assertEquals(expectedValue, spv.readToString());

        // now save a smaller value, making sure it gets truncated
        expectedValue = "small";
        propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedValue.getBytes()),
                String.class,
                (long) expectedValue.length()
        );
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "largeProp", propLarge, metadata, timestamp + 1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        spv = (StreamingPropertyValue) v1.getPropertyValue("key1", "largeProp");
        assertEquals(expectedValue, spv.readToString());
    }

    protected boolean isInputStreamMarkResetSupported() {
        return true;
    }

    @Test
    public void testStreamingPropertyValueMarkReset() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = "abcdefghijk";
        PropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();

        byte[] buffer = new byte[15];
        int sizeRead = in.read(buffer);
        assertEquals(11, sizeRead);
        assertEquals("abcdefghijk", new String(buffer, 0, sizeRead));

        in.reset();
        buffer = new byte[3];
        sizeRead = in.read(buffer);
        assertEquals(3, sizeRead);
        assertEquals("abc", new String(buffer, 0, sizeRead));
        assertEquals('d', (char) in.read());
        assertEquals('e', (char) in.read());

        in.mark(32);
        buffer = new byte[5];
        sizeRead = in.read(buffer);
        assertEquals(5, sizeRead);
        assertEquals("fghij", new String(buffer, 0, sizeRead));

        in.reset();
        buffer = new byte[10];
        sizeRead = in.read(buffer);
        assertEquals(6, sizeRead);
        assertEquals("fghijk", new String(buffer, 0, sizeRead));

        assertEquals(-1, in.read(buffer));

        in.reset();
        buffer = new byte[10];
        sizeRead = in.read(buffer);
        assertEquals(6, sizeRead);
        assertEquals("fghijk", new String(buffer, 0, sizeRead));
    }

    @Test
    public void testStreamingPropertyValueMarkResetLargeReads() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        PropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();
        int amountToRead = expectedLargeValueBytes.length - 8;
        byte[] buffer = null;
        while (amountToRead > 0) {
            buffer = new byte[amountToRead];
            amountToRead -= in.read(buffer);
        }

        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 9), (char) buffer[buffer.length - 1]);
        in.mark(32);
        buffer = new byte[2];
        int sizeRead = in.read(buffer);
        assertEquals(2, sizeRead);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 8), (char) buffer[0]);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 7), (char) buffer[1]);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 6), (char) in.read());

        in.reset();
        sizeRead = in.read(buffer);
        assertEquals(2, sizeRead);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 8), (char) buffer[0]);
    }

    @Test
    public void testStreamingPropertyValueResetMutlipleLargeReadsUntilEnd() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        PropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();
        in.mark(2);
        for (int i = 0; i < 3; i++) {
            int totalBytesRead = 0;
            while (in.read() >= 0) {
                totalBytesRead++;
                assertTrue("Read past end of input stream", totalBytesRead <= expectedLargeValueBytes.length);
            }
            assertEquals("Read unexpected number of bytes on loop: " + i, expectedLargeValueBytes.length, totalBytesRead);
            assertEquals(-1, in.read());
            in.reset();
        }
    }

    @Test
    public void testAddVertexPropertyWithMetadata() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        AtomicInteger vertexCount = new AtomicInteger();
        AtomicInteger vertexPropertyCount = new AtomicInteger();
        graph.visitElements(new DefaultGraphVisitor() {
            @Override
            public void visitVertex(Vertex vertex) {
                vertexCount.incrementAndGet();
            }

            @Override
            public void visitProperty(Element element, Property property) {
                vertexPropertyCount.incrementAndGet();
            }
        }, AUTHORIZATIONS_A);
        assertEquals(1, vertexCount.get());
        assertEquals(1, vertexPropertyCount.get());

        Vertex v = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
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

        v = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
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

        v = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
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
        clearGraphEvents();

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
        clearGraphEvents();

        v = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Property prop1_propid1a = v.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v.getProperty("propid1b", "prop1");
        v.deleteProperties("prop1", AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(v.getProperties()));
        v = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v.getProperties()));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop2", "value2a").vertices()));
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1a").vertices()));
        assertEvents(
                new DeletePropertyEvent(graph, v, prop1_propid1a),
                new DeletePropertyEvent(graph, v, prop1_propid1b)
        );
        clearGraphEvents();

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
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        clearGraphEvents();

        // delete multiple properties
        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Property prop1_propid1a = v1.getProperty("propid1a", "prop1");
        Property prop1_propid1b = v1.getProperty("propid1b", "prop1");
        v1.prepareMutation()
                .deleteProperties("prop1")
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(v1.getProperties()));
        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getProperties()));

        Assert.assertEquals(1, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop2", "value2a").vertices()));
        Assert.assertEquals(0, count(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "value1a").vertices()));
        assertEvents(
                new DeletePropertyEvent(graph, v1, prop1_propid1a),
                new DeletePropertyEvent(graph, v1, prop1_propid1b)
        );
        clearGraphEvents();

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
        clearGraphEvents();

        // delete property from edge
        Edge e1 = graph.getEdge("e1", FetchHints.ALL, AUTHORIZATIONS_A);
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
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(2, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        Vertex v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(1, v2.getEdgesSummary(AUTHORIZATIONS_A).getCountOfEdges());

        graph.softDeleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));
        assertEquals(0, count(graph.query(AUTHORIZATIONS_A).has("name1", "value1").vertices()));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(0, v2.getEdgesSummary(AUTHORIZATIONS_A).getCountOfEdges());

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
    public void testReAddingSoftDeletedVertex() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "p1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        assertEquals(VISIBILITY_A.getVisibilityString(), v1.getVisibility().getVisibilityString());

        graph.softDeleteVertex(v1, AUTHORIZATIONS_A);
        graph.flush();

        graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(VISIBILITY_A.getVisibilityString(), v1.getVisibility().getVisibilityString());
        assertEquals(0, count(v1.getProperties()));

        graph.softDeleteVertex(v1, AUTHORIZATIONS_A);
        graph.flush();

        graph.prepareVertex("v1", VISIBILITY_A_AND_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1);
        assertEquals(VISIBILITY_A_AND_B.getVisibilityString(), v1.getVisibility().getVisibilityString());

        graph.softDeleteVertex(v1, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1);
        assertEquals(VISIBILITY_EMPTY.getVisibilityString(), v1.getVisibility().getVisibilityString());
    }

    @Test
    public void testGetSoftDeletedElementWithFetchHintsAndTimestamp() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        long beforeDeleteTime = IncreasingTime.currentTimeMillis();
        graph.softDeleteEdge(e1, AUTHORIZATIONS_A);
        graph.softDeleteVertex(v1, AUTHORIZATIONS_A);
        graph.flush();

        assertNull(graph.getEdge(e1.getId(), AUTHORIZATIONS_A));
        assertNull(graph.getEdge(e1.getId(), graph.getDefaultFetchHints(), AUTHORIZATIONS_A));
        assertNull(graph.getEdge(e1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), graph.getDefaultFetchHints(), AUTHORIZATIONS_A));
        assertNull(graph.getVertex(v1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A));

        assertNotNull(graph.getEdge(e1.getId(), graph.getDefaultFetchHints(), beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getEdge(e1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getVertex(v1.getId(), graph.getDefaultFetchHints(), beforeDeleteTime, AUTHORIZATIONS_A));
        assertNotNull(graph.getVertex(v1.getId(), FetchHints.ALL_INCLUDING_HIDDEN, beforeDeleteTime, AUTHORIZATIONS_A));
    }

    @Test
    public void testSoftDeleteEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        graph.softDeleteEdge(e1, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v1.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v1.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v2 = graph.getVertex("v2", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
        assertEquals(0, count(v2.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(v2.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = graph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v3.getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(1, count(v3.getVertexIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

        v3 = graph.getVertex("v3", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
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
        ElementBuilder<Edge> elementBuilder = graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_B)
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

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
        v1.softDeleteProperty("key1", "name1", AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
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

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
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
        List<Property> properties = IterableUtils.toList(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));

        long beforeMarkPropertyVisibleTimestamp = t;
        t += 10;

        v1.markPropertyVisible("key1", "firstName", VISIBILITY_A, t, VISIBILITY_B, AUTHORIZATIONS_A_AND_B);
        t += 10;
        properties = IterableUtils.toList(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties());
        assertEquals(1, count(properties));
        graph.flush();

        v1 = graph.getVertex("v1", graph.getDefaultFetchHints(), beforeMarkPropertyVisibleTimestamp, AUTHORIZATIONS_A_AND_B);
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
        assertVertexIds(vertices, "v1", "v2");
        graph.flush();

        if (graph instanceof GraphWithSearchIndex) {
            ((GraphWithSearchIndex) graph).getSearchIndex().addElements(graph, vertices, AUTHORIZATIONS_A_AND_B);
            assertVertexIds(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "v1").vertices(), "v1");
            assertVertexIds(graph.query(AUTHORIZATIONS_A_AND_B).has("prop1", "v2").vertices(), "v2");
        }
    }

    @Test
    public void testAddExtendedDataRows() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        if (graph instanceof GraphWithSearchIndex) {
            SearchIndex searchIndex = ((GraphWithSearchIndex) graph).getSearchIndex();
            searchIndex.truncate(graph);
            searchIndex.flush(graph);

            Iterable<ExtendedDataRow> extendedData = graph.getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A);
            searchIndex.addExtendedData(graph, extendedData, AUTHORIZATIONS_A);
            graph.flush();
        }

        QueryResultsIterable<ExtendedDataRow> rows = graph.query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = graph.query(AUTHORIZATIONS_A)
                .has("name", "value1")
                .extendedDataRows();
        assertResultsCount(1, 1, rows);

        ExtendedDataRow row = IterableUtils.single(rows);
        assertEquals("v1", row.getId().getElementId());
        assertEquals("table1", row.getId().getTableName());
        assertEquals("row1", row.getId().getRowId());
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
        assertVertexIds(vertices, "a", "aa", "az");

        vertices = sortById(toList(graph.getVerticesWithPrefix("b", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices, "b");

        vertices = sortById(toList(graph.getVerticesWithPrefix("c", AUTHORIZATIONS_ALL)));
        assertVertexIds(vertices);
    }

    @Test
    public void testGetVerticesInRange() {
        graph.addVertex("a", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("aa", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("az", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addVertex("b", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.flush();

        List<Vertex> vertices = toList(graph.getVerticesInRange(new Range(null, "a"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices);

        vertices = toList(graph.getVerticesInRange(new Range(null, "b"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az");

        vertices = toList(graph.getVerticesInRange(new Range(null, "bb"), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az", "b");

        vertices = toList(graph.getVerticesInRange(new Range(null, null), AUTHORIZATIONS_ALL));
        assertVertexIds(vertices, "a", "aa", "az", "b");
    }

    @Test
    public void testGetEdgesInRange() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("a", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("aa", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("az", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.addEdge("b", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_ALL);
        graph.flush();

        List<Edge> edges = toList(graph.getEdgesInRange(new Range(null, "a"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges);

        edges = toList(graph.getEdgesInRange(new Range(null, "b"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az");

        edges = toList(graph.getEdgesInRange(new Range(null, "bb"), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az", "b");

        edges = toList(graph.getEdgesInRange(new Range(null, null), AUTHORIZATIONS_ALL));
        assertEdgeIds(edges, "a", "aa", "az", "b");
    }

    @Test
    public void testGetEdgesWithIds() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "e1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1a", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "e1a", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "e2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", v2, v3, LABEL_LABEL1, VISIBILITY_A)
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

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
        assertNotNull("could not find v1", v1);
        assertEquals(2, count(v1.getProperties()));
        assertEquals(25, v1.getPropertyValue("k1", "age"));
        assertEquals(30, v1.getPropertyValue("k2", "age"));

        v1 = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_ALL);
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
        graph.addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
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
        assertNull("found v1 but shouldn't have", graph.getVertex("v1", graph.getDefaultFetchHints(), AUTHORIZATIONS_A));
        Vertex v1Hidden = graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A);
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
        Edge e1 = graph.addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
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
        Assert.assertEquals(1, count(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));

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
        Assert.assertEquals(0, count(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(0, count(graph.findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A)));
        assertNull("found e1 but shouldn't have", graph.getEdge("v1tov2", graph.getDefaultFetchHints(), AUTHORIZATIONS_A_AND_B));
        Edge e1Hidden = graph.getEdge("v1tov2", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B);
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
        Assert.assertEquals(1, count(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B)));
        Assert.assertEquals(1, count(graph.findPaths(new FindPathOptions("v1", "v3", 10), AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testSearchingForHiddenEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Edge e1 = graph.addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        Edge e2 = graph.addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        graph.markEdgeHidden(e1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        graph.flush();

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .edges(propertiesFetchHints);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());

        edges = graph.query(AUTHORIZATIONS_A_AND_B)
                .edges(propertiesFetchHints);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e2.getId());

        graph.markEdgeVisible(e1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        graph.flush();

        edges = graph.query(AUTHORIZATIONS_A_AND_B)
                .edges(propertiesFetchHints);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());
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

        List<Property> hiddenProperties = toList(graph.getVertex("v1", FetchHints.ALL_INCLUDING_HIDDEN, AUTHORIZATIONS_A_AND_B).getProperties());
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

    @Test
    public void testSearchingForHiddenVertices() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A)
                .addPropertyValue("key1", "prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        graph.markVertexHidden(v1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        graph.flush();

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("prop1", "value1")
                .vertices(propertiesFetchHints);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId());

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .vertices(propertiesFetchHints);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v2.getId());

        graph.markVertexVisible(v1, VISIBILITY_B, AUTHORIZATIONS_ALL);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .vertices(propertiesFetchHints);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId());
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
        graph.addEdge("w1-e1", w1, e1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("w2-e1", w2, e1, LABEL_LABEL1, VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge("e1-e2", e1, e2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A);
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
        Edge addedEdge = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();
        assertNotNull(addedEdge);
        assertEquals("e1", addedEdge.getId());
        assertEquals(LABEL_LABEL1, addedEdge.getLabel());
        assertEquals("v1", addedEdge.getVertexId(Direction.OUT));
        assertEquals(v1, addedEdge.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", addedEdge.getVertexId(Direction.IN));
        assertEquals(v2, addedEdge.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, addedEdge.getVisibility());

        EdgeVertices addedEdgeVertices = addedEdge.getVertices(AUTHORIZATIONS_A);
        assertEquals(v1, addedEdgeVertices.getOutVertex());
        assertEquals(v2, addedEdgeVertices.getInVertex());

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        FetchHints inEdgeRefsFetchHints = FetchHints.builder()
                .setIncludeInEdgeRefs(true)
                .build();
        FetchHints outEdgeRefsFetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();

        graph.getVertex("v1", FetchHints.NONE, AUTHORIZATIONS_A);
        graph.getVertex("v1", graph.getDefaultFetchHints(), AUTHORIZATIONS_A);
        graph.getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        graph.getVertex("v1", FetchHints.EDGE_REFS, AUTHORIZATIONS_A);
        graph.getVertex("v1", inEdgeRefsFetchHints, AUTHORIZATIONS_A);
        graph.getVertex("v1", outEdgeRefsFetchHints, AUTHORIZATIONS_A);

        graph.getEdge("e1", FetchHints.NONE, AUTHORIZATIONS_A);
        graph.getEdge("e1", graph.getDefaultFetchHints(), AUTHORIZATIONS_A);
        graph.getEdge("e1", propertiesFetchHints, AUTHORIZATIONS_A);

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_B);
        assertNull(e);

        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals(LABEL_LABEL1, e.getLabel());
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
        graph.addEdge("e1to2label1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to2label2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2to1", v2.getId(), v1.getId(), LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);

        Assert.assertEquals(3, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.BOTH, LABEL_LABEL1, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.OUT, LABEL_LABEL1, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, LABEL_LABEL1, AUTHORIZATIONS_A)));
        Assert.assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));
        Assert.assertEquals(2, count(v1.getEdges(v2, Direction.OUT, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));
        Assert.assertEquals(1, count(v1.getEdges(v2, Direction.IN, new String[]{LABEL_LABEL1, LABEL_LABEL2}, AUTHORIZATIONS_A)));

        Assert.assertArrayEquals(new String[]{LABEL_LABEL1, LABEL_LABEL2}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels(), String.class));
        Assert.assertArrayEquals(new String[]{LABEL_LABEL1}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels(), String.class));
        Assert.assertArrayEquals(new String[]{LABEL_LABEL1, LABEL_LABEL2}, IterableUtils.toArray(v1.getEdgesSummary(AUTHORIZATIONS_A).getEdgeLabels(), String.class));
    }

    @Test
    public void testGetEdgeVertexPairs() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v2_label1 = graph.addEdge("v1_to_v2_label1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v2_label2 = graph.addEdge("v1_to_v2_label2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge v1_to_v3_label2 = graph.addEdge("v1_to_v3_label2", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);

        List<EdgeVertexPair> pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(3, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label1, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));

        pairs = toList(v1.getEdgeVertexPairs(Direction.BOTH, LABEL_LABEL2, AUTHORIZATIONS_A));
        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v2_label2, v2)));
        assertTrue(pairs.contains(new EdgeVertexPair(v1_to_v3_label2, v3)));
    }

    @Test
    public void testAddEdgeWithProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge addedEdge = graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
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
            getGraph().prepareEdge("e1", outVertexId, inVertexId, LABEL_LABEL1, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_ALL);
            fail("should throw an exception");
        } catch (Exception ex) {
            assertNotNull(ex);
        }

        try {
            Vertex outVertex = null;
            Vertex inVertex = null;
            getGraph().prepareEdge("e1", outVertex, inVertex, LABEL_LABEL1, VISIBILITY_EMPTY)
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
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
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
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propA", "valueA", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(LABEL_LABEL1, e.getLabel());
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        Assert.assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(LABEL_LABEL1, IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(LABEL_LABEL1, IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));

        e.prepareMutation()
                .alterEdgeLabel(LABEL_LABEL2)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(LABEL_LABEL2, e.getLabel());
        Assert.assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        Assert.assertEquals(LABEL_LABEL2, IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals(LABEL_LABEL2, IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));

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
        Assert.assertEquals("label3", IterableUtils.single(v1.getEdgesSummary(AUTHORIZATIONS_A).getOutEdgeLabels()));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(v2.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        Assert.assertEquals("label3", IterableUtils.single(v2.getEdgesSummary(AUTHORIZATIONS_A).getInEdgeLabels()));
    }

    @Test
    public void testDeleteEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge addedEdge = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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
        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_B, AUTHORIZATIONS_B);
        graph.flush();

        Iterable<Edge> aEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A);
        Assert.assertEquals(1, count(aEdges));
        assertEquals(LABEL_LABEL1, IterableUtils.single(aEdges).getLabel());

        Iterable<Edge> bEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_B);
        Assert.assertEquals(1, count(bEdges));
        assertEquals(LABEL_LABEL2, IterableUtils.single(bEdges).getLabel());

        Iterable<Edge> allEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B);
        Assert.assertEquals(2, count(allEdges));
    }

    @Test
    public void testGraphQueryPagingForUniqueIdsSortedOrder() {
        String namePropertyName = "first.name";
        graph.defineProperty(namePropertyName).dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", namePropertyName, "B", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .addPropertyValue("k1", namePropertyName, "A", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .addPropertyValue("k1", namePropertyName, "C", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<String> idsIterable = graph.query(AUTHORIZATIONS_A)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .skip(0)
                .limit(1)
                .vertexIds();
        assertIdsAnyOrder(idsIterable, "v1");
        assertResultsCount(1, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .skip(1)
                .limit(1)
                .vertexIds();
        assertIdsAnyOrder(idsIterable, "v2");

        idsIterable = graph.query(AUTHORIZATIONS_A)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .skip(2)
                .limit(1)
                .vertexIds();
        assertIdsAnyOrder(idsIterable, "v3");

        idsIterable = graph.query(AUTHORIZATIONS_A).sort(namePropertyName, SortDirection.ASCENDING).vertexIds();
        assertResultsCount(3, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).limit((Long) null).vertexIds();
        assertResultsCount(3, 3, idsIterable);

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A)
                .sort(namePropertyName, SortDirection.ASCENDING)
                .skip(0)
                .limit(1)
                .vertices());
        assertEquals(1, vertices.size());
        assertEquals("v2", vertices.get(0).getId());
    }

    @Test
    public void testGraphQueryForIds() {
        String namePropertyName = "first.name";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", namePropertyName, "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .addExtendedData("table1", "row1", namePropertyName, "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", namePropertyName, "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<String> idsIterable = graph.query(AUTHORIZATIONS_A).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3");
        assertResultsCount(3, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).skip(1).vertexIds();
        assertResultsCount(2, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).limit(1).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).skip(1).limit(1).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).skip(3).vertexIds();
        assertResultsCount(0, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).skip(2).limit(2).vertexIds();
        assertResultsCount(1, 3, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).edgeIds();
        assertIdsAnyOrder(idsIterable, "e1", "e2");
        assertResultsCount(2, 2, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1).edgeIds();
        assertIdsAnyOrder(idsIterable, "e1");
        assertResultsCount(1, 1, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2).edgeIds();
        assertResultsCount(2, 2, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).elementIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3", "e1", "e2");
        assertResultsCount(5, 5, idsIterable);

        assumeTrue("FetchHints.NONE vertex queries are not supported", isFetchHintNoneVertexQuerySupported());

        idsIterable = graph.query(AUTHORIZATIONS_A).has(namePropertyName).vertexIds();
        assertIdsAnyOrder(idsIterable, "v1");
        assertResultsCount(1, 1, idsIterable);

        QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds = graph.query(AUTHORIZATIONS_A).hasExtendedData("table1").extendedDataRowIds();
        List<String> rowIds = stream(extendedDataRowIds).map(ExtendedDataRowId::getRowId).collect(Collectors.toList());
        assertIdsAnyOrder(rowIds, "row1", "row2");
        assertResultsCount(2, 2, extendedDataRowIds);

        idsIterable = graph.query(AUTHORIZATIONS_A).hasNot(namePropertyName).vertexIds();
        assertIdsAnyOrder(idsIterable, "v2", "v3");
        assertResultsCount(2, 2, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).has("notSetProp").vertexIds();
        assertResultsCount(0, 0, idsIterable);

        idsIterable = graph.query(AUTHORIZATIONS_A).hasNot("notSetProp").vertexIds();
        assertIdsAnyOrder(idsIterable, "v1", "v2", "v3");
        assertResultsCount(3, 3, idsIterable);

        try {
            graph.query(AUTHORIZATIONS_A).has("notSetProp", Compare.NOT_EQUAL, 5).vertexIds();
            fail("Value queries should not be allowed for properties that are not defined.");
        } catch (VertexiumException ve) {
            assertEquals("Could not find property definition for property name: notSetProp", ve.getMessage());
        }
    }

    @Test
    public void testGraphQueryForEdgesUsingInOutVertexIds() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e3", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .has(Edge.OUT_VERTEX_ID_PROPERTY_NAME, "v1")
                .has(Edge.IN_VERTEX_ID_PROPERTY_NAME, "v2")
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1");

        edges = graph.query(AUTHORIZATIONS_A)
                .has(Edge.OUT_VERTEX_ID_PROPERTY_NAME, "v1")
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1", "e2");
    }

    @Test
    public void testGraphQueryForEdgesUsingEdgeLabel() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e3", v3, v1, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .has(Edge.LABEL_PROPERTY_NAME, LABEL_LABEL1)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e1");

        edges = graph.query(AUTHORIZATIONS_A)
                .has(Edge.LABEL_PROPERTY_NAME, LABEL_LABEL2)
                .edges();
        assertEdgeIdsAnyOrder(edges, "e2", "e3");
    }

    @Test
    public void testGraphQuery() {
        String namePropertyName = "first.name";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", namePropertyName, "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
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

        edges = graph.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1).edges();
        assertResultsCount(1, 1, edges);

        edges = graph.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2).edges();
        assertResultsCount(2, 2, edges);

        QueryResultsIterable<Element> elements = graph.query(AUTHORIZATIONS_A).elements();
        assertResultsCount(4, 4, elements);

        vertices = graph.query(AUTHORIZATIONS_A).has(namePropertyName).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasNot(namePropertyName).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("notSetProp").vertices();
        assertResultsCount(0, 0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasNot("notSetProp").vertices();
        assertResultsCount(2, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasId("v1").vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasId("v1", "v2").vertices();
        assertResultsCount(2, 2, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).hasId("v1", "v2").hasId("v1").vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A).hasId("v1").hasId("v2").vertices();
        assertResultsCount(0, 0, vertices);

        edges = graph.query(AUTHORIZATIONS_A).hasId("e1").edges();
        assertResultsCount(1, 1, edges);

        edges = graph.query(AUTHORIZATIONS_A).hasId("e1", "e2").edges();
        assertResultsCount(2, 2, edges);

        try {
            graph.query(AUTHORIZATIONS_A).has("notSetProp", Compare.NOT_EQUAL, 5).vertices();
            fail("Value queries should not be allowed for properties that are not defined.");
        } catch (VertexiumException ve) {
            assertEquals("Could not find property definition for property name: notSetProp", ve.getMessage());
        }
    }

    @Test
    public void testGraphQueryWithBoolean() {
        graph.defineProperty("boolean").dataType(Boolean.class).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "boolean", true, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query("zzzzz", AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("boolean", true).vertices();
        assertResultsCount(1, 1, vertices);

        vertices = graph.query(AUTHORIZATIONS_A).has("boolean", false).vertices();
        assertResultsCount(0, 0, vertices);
    }

    @Test
    public void testGraphQueryWithFetchHints() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.prepareVertex("v2", VISIBILITY_A)
                .addPropertyValue("k1", "name", "matt", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.prepareVertex("v3", VISIBILITY_A)
                .addPropertyValue("k1", "name", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.flush();

        assertTrue(graph.getVertexCount(AUTHORIZATIONS_A) == 3);

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", "joe")
                .vertices(propertiesFetchHints);

        assertResultsCount(2, 2, vertices);

        assumeTrue("FetchHints.NONE vertex queries are not supported", isFetchHintNoneVertexQuerySupported());

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", "joe")
                .vertices(FetchHints.NONE);

        assertResultsCount(2, 2, vertices);
    }

    protected boolean isFetchHintNoneVertexQuerySupported() {
        return true;
    }

    @Test
    public void testSaveElementMutations() {
        List<ElementMutation> mutations = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ElementBuilder<Vertex> m = graph.prepareVertex("v" + i, VISIBILITY_A)
                    .addPropertyValue("k1", "name", "joe", VISIBILITY_A)
                    .addExtendedData("table1", "row1", "col1", "extended", VISIBILITY_A);
            mutations.add(m);
        }
        List<Element> saveVertices = toList(graph.saveElementMutations(mutations, AUTHORIZATIONS_ALL));
        graph.flush();

        assertEvents(
                new AddVertexEvent(graph, (Vertex) saveVertices.get(0)),
                new AddPropertyEvent(graph, saveVertices.get(0), saveVertices.get(0).getProperty("k1", "name")),
                new AddExtendedDataEvent(graph, saveVertices.get(0), "table1", "row1", "col1", null, "extended", VISIBILITY_A),
                new AddVertexEvent(graph, (Vertex) saveVertices.get(1)),
                new AddPropertyEvent(graph, saveVertices.get(1), saveVertices.get(1).getProperty("k1", "name")),
                new AddExtendedDataEvent(graph, saveVertices.get(1), "table1", "row1", "col1", null, "extended", VISIBILITY_A)
        );
        clearGraphEvents();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_ALL).vertices();
        assertResultsCount(2, 2, vertices);

        QueryResultsIterable<? extends VertexiumObject> items = graph.query(AUTHORIZATIONS_ALL)
                .has("col1", "extended")
                .search();
        assertResultsCount(2, 2, items);

        mutations.clear();
        mutations.add(((Vertex) saveVertices.get(0)).prepareMutation());
        graph.saveElementMutations(mutations, AUTHORIZATIONS_ALL);
        graph.flush();

        assertEvents();
    }

    @Test
    public void testAddValuesToExistingProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        graph.defineProperty("p1").dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        v1.addPropertyValue("k1", "p1", "val1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val1").vertexIds(), "v1");

        v1.addPropertyValue("k2", "p1", "val2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val1").vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val2").vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val3").vertexIds());

        v1.addPropertyValue("k1", "p1", "val3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val3").vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val2").vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "val1").vertexIds());
    }

    @Test
    public void testRemoveValuesFromMultivalueProperties() {
        graph.defineProperty("p1").dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("k1", "p1", "v1", VISIBILITY_A)
                .addPropertyValue("k2", "p1", "v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v1").vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v2").vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v3").vertexIds());

        v1.prepareMutation()
                .addPropertyValue("k3", "p1", "v3", VISIBILITY_A)
                .deleteProperty("k1", "p1", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v2").vertexIds(), "v1");
        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v3").vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v1").vertexIds());

        v1.deleteProperty("k2", "p1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();

        assertIdsAnyOrder(getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v3").vertexIds(), "v1");
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v1").vertexIds());
        assertResultsCount(0, 0, getGraph().query(AUTHORIZATIONS_ALL).has("p1", "v2").vertexIds());
    }

    @Test
    public void testGraphQueryWithQueryString() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.setProperty("description", "This is vertex 1 - dog.", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v2.setProperty("description", "This is vertex 2 - cat.", VISIBILITY_B, AUTHORIZATIONS_ALL);
        Edge e1 = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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
        Edge e1 = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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
        String agePropertyName = "age.property";
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "hello", VISIBILITY_A)
                .setProperty(agePropertyName, 25, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text", "world", VISIBILITY_A)
                .setProperty(agePropertyName, 30, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .hasNot(agePropertyName)
                    .vertices();
            Assert.assertEquals(0, count(vertices));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.EQUAL, 25)
                .has("birthDate", Compare.EQUAL, createDate(1989, 1, 5))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("hello", AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.EQUAL, 25)
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
                .has(agePropertyName, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(25, (int) toList(vertices).get(0).getPropertyValue(agePropertyName));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .hasNot(agePropertyName, 25)
                    .vertices();
            Assert.assertEquals(1, count(vertices));
            Assert.assertEquals(30, (int) toList(vertices).get(0).getPropertyValue(agePropertyName));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.GREATER_THAN_EQUAL, 25)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Contains.IN, new Integer[]{25})
                .vertices();
        Assert.assertEquals(1, count(vertices));
        Assert.assertEquals(25, (int) toList(vertices).get(0).getPropertyValue(agePropertyName));

        try {
            vertices = graph.query(AUTHORIZATIONS_A)
                    .has(agePropertyName, Contains.NOT_IN, new Integer[]{25})
                    .vertices();
            Assert.assertEquals(1, count(vertices));
            Assert.assertEquals(30, (int) toList(vertices).get(0).getPropertyValue(agePropertyName));
        } catch (VertexiumNotSupportedException ex) {
            LOGGER.warn("skipping. Not supported", ex);
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Contains.IN, new Integer[]{25, 30})
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.GREATER_THAN, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.LESS_THAN, 26)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.LESS_THAN_EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(agePropertyName, Compare.NOT_EQUAL, 25)
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("lastAccessed", Compare.EQUAL, new DateOnly(2014, 2, 24))
                .vertices();
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("*", AUTHORIZATIONS_A)
                .has(agePropertyName, Contains.IN, new Integer[]{25, 30})
                .vertices();
        Assert.assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQueryMultiPropertyHas() {
        graph.defineProperty("unusedFloatProp").dataType(Float.class).define();
        graph.defineProperty("unusedDateProp").dataType(Date.class).define();
        graph.defineProperty("unusedStringProp").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();

        String agePropertyName = "age.property";
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "hello", VISIBILITY_A)
                .setProperty("text2", "foo", VISIBILITY_A)
                .setProperty("text3", "bar", VISIBILITY_A)
                .setProperty(agePropertyName, 25, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9544, -77.3464, "Reston, VA"), VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text", "world", VISIBILITY_A)
                .setProperty("text2", "foo", VISIBILITY_A)
                .setProperty(agePropertyName, 30, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has(String.class)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .hasNot(String.class)
                .vertices();
        assertResultsCount(0, vertices);

        try {
            graph.query(AUTHORIZATIONS_A)
                    .has(Double.class)
                    .vertices();
            fail("Should not allow searching for a dataType that there are no mappings for");
        } catch (VertexiumException ve) {
            // expected
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Float.class)
                .vertices();
        assertResultsCount(0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A)
                .hasNot(Float.class)
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Arrays.asList("text3", "unusedStringProp"))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A)
                .hasNot(Arrays.asList("text3", "unusedStringProp"))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(String.class, Compare.EQUAL, "hello")
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(String.class, Compare.EQUAL, "foo")
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Arrays.asList("text", "text2"), Compare.EQUAL, "hello")
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Arrays.asList("text", "text2"), Compare.EQUAL, "foo")
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Arrays.asList("text"), Compare.EQUAL, "foo")
                .vertices();
        assertResultsCount(0, vertices);

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Number.class, Compare.EQUAL, 25)
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Date.class, Compare.GREATER_THAN, createDate(2014, 2, 25, 0, 0, 0))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Date.class, Compare.LESS_THAN, createDate(2014, 2, 25, 0, 0, 0))
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Date.class, Compare.LESS_THAN, new DateOnly(1985, 1, 5))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Date.class, Compare.GREATER_THAN, new DateOnly(2000, 1, 1))
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(GeoShape.class, GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 1))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(GeoPoint.class, GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 1))
                .vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has(GeoPoint.class, GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 25))
                .vertices();
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2");

        try {
            graph.query(AUTHORIZATIONS_A)
                    .has(Date.class, GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 1))
                    .vertices()
                    .getTotalHits();
            fail("GeoCompare searches should not be allowed for date fields");
        } catch (VertexiumException e) {
            // expected
        }

        try {
            graph.query(AUTHORIZATIONS_A)
                    .has(Double.class, Compare.EQUAL, 25)
                    .vertices();
            fail("Should not allow searching for a dataType that there are no mappings for");
        } catch (VertexiumException e) {
            // expected
        }

        // If given a property that is defined in the graph but never used, return no results
        vertices = graph.query(AUTHORIZATIONS_A)
                .has(Float.class, Compare.EQUAL, 25)
                .vertices();
        assertResultsCount(0, vertices);
    }

    @Test
    public void testGraphQueryHasAuthorization() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "hello", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text.with.dots", "world", VISIBILITY_B)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v3", VISIBILITY_C)
                .setProperty("text", "world", VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_ALL)
                .hasAuthorization(VISIBILITY_A_STRING)
                .vertices();
        assertResultsCount(3, 3, vertices);
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3");

        vertices = graph.query(AUTHORIZATIONS_ALL)
                .hasAuthorization(VISIBILITY_B_STRING)
                .vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIdsAnyOrder(vertices, "v2");

        vertices = graph.query(AUTHORIZATIONS_ALL)
                .hasAuthorization(VISIBILITY_C_STRING)
                .vertices();
        assertResultsCount(1, 1, vertices);
        assertVertexIdsAnyOrder(vertices, "v3");
    }

    @Test
    public void testGraphQueryHasAuthorizationWithHidden() {
        Vertex v1 = graph.addVertex("v1", Visibility.EMPTY, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", Visibility.EMPTY, AUTHORIZATIONS_A);
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("junit", "name", "value", VISIBILITY_B)
                .save(AUTHORIZATIONS_B_AND_C);
        Edge e1 = graph.addEdge("e1", v1.getId(), v2.getId(), "junit edge", Visibility.EMPTY, AUTHORIZATIONS_A);
        graph.flush();

        graph.markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        v3.markPropertyHidden("junit", "name", VISIBILITY_B, VISIBILITY_C, AUTHORIZATIONS_B_AND_C);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).vertices(FetchHints.ALL);
        assertResultsCount(0, vertices);

        QueryResultsIterable<String> vertexIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).vertexIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, vertexIds);

        vertexIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).vertexIds();
        assertResultsCount(0, 0, vertexIds);

        vertices = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId());

        vertices = graph.query(AUTHORIZATIONS_B_AND_C).hasAuthorization(VISIBILITY_C_STRING).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, vertices);
        assertVertexIdsAnyOrder(vertices, v3.getId());

        vertexIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, vertexIds);
        assertIdsAnyOrder(vertexIds, v1.getId());

        vertexIds = graph.query(AUTHORIZATIONS_B_AND_C).hasAuthorization(VISIBILITY_C_STRING).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, vertexIds);
        assertIdsAnyOrder(vertexIds, v3.getId());

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).edges(FetchHints.ALL);
        assertResultsCount(0, edges);

        QueryResultsIterable<String> edgeIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).edgeIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, edgeIds);

        edgeIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).edgeIds();
        assertResultsCount(0, 0, edgeIds);

        edges = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).edges(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId());

        edgeIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).edgeIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e1.getId());

        QueryResultsIterable<Element> elements = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).elements(FetchHints.ALL);
        assertResultsCount(0, elements);

        QueryResultsIterable<String> elementIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).elementIds(IdFetchHint.NONE);
        assertResultsCount(0, 0, elementIds);

        elementIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).elementIds();
        assertResultsCount(0, 0, elementIds);

        elements = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).elements(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, elements);
        assertElementIdsAnyOrder(elements, v1.getId(), e1.getId());

        elementIds = graph.query(AUTHORIZATIONS_A).hasAuthorization(VISIBILITY_A_STRING).elementIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, 2, elementIds);
        assertIdsAnyOrder(elementIds, v1.getId(), e1.getId());
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
        String agePropertyName = "age.property";
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("name", "v1", VISIBILITY_A)
                .setProperty(agePropertyName, 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("name", "v2", VISIBILITY_A)
                .addPropertyValue("k1", agePropertyName, 30, VISIBILITY_A)
                .addPropertyValue("k2", agePropertyName, 35, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("name", "v3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has(agePropertyName)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .hasNot(agePropertyName)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryIn() {
        String namePropertyName = "full.name";
        graph.defineProperty("age").dataType(Integer.class).sortable(true).define();
        graph.defineProperty(namePropertyName).dataType(String.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty(namePropertyName, "joe ferner", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty(namePropertyName, "bob smith", VISIBILITY_B)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty(namePropertyName, "tom thumb", VISIBILITY_A)
                .setProperty("age", 30, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<String> strings = new ArrayList<>();
        strings.add("joe ferner");
        strings.add("tom thumb");
        Iterable<Vertex> results = graph.query(AUTHORIZATIONS_A_AND_B).has(namePropertyName, Contains.IN, strings).vertices();
        assertEquals(2, ((IterableWithTotalHits) results).getTotalHits());
        assertVertexIdsAnyOrder(results, "v1", "v3");
    }

    @Test
    public void testGraphQuerySort() {
        String namePropertyName = "first.name";
        String agePropertyName = "age";
        String genderPropertyName = "gender";
        graph.defineProperty(agePropertyName).dataType(Integer.class).sortable(true).define();
        graph.defineProperty(namePropertyName).dataType(String.class).sortable(true).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        graph.defineProperty(genderPropertyName).dataType(String.class).sortable(true).textIndexHint(TextIndexHint.FULL_TEXT, TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty(namePropertyName, "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty(namePropertyName, "bob", VISIBILITY_B)
                .setProperty(agePropertyName, 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty(namePropertyName, "tom", VISIBILITY_A)
                .setProperty(agePropertyName, 30, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_A)
                .setProperty(namePropertyName, "tom", VISIBILITY_A)
                .setProperty(agePropertyName, 35, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL2, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", "v1", "v2", LABEL_LABEL3, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(agePropertyName, SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, "v2", "v3", "v4", "v1");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(agePropertyName, SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, "v4", "v3", "v2", "v1");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(namePropertyName, SortDirection.ASCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));
        assertEquals("v2", vertices.get(0).getId());
        assertEquals("v1", vertices.get(1).getId());
        assertTrue(vertices.get(2).getId().equals("v3") || vertices.get(2).getId().equals("v4"));
        assertTrue(vertices.get(3).getId().equals("v3") || vertices.get(3).getId().equals("v4"));

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(namePropertyName, SortDirection.DESCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));
        assertTrue(vertices.get(0).getId().equals("v3") || vertices.get(0).getId().equals("v4"));
        assertTrue(vertices.get(1).getId().equals("v3") || vertices.get(1).getId().equals("v4"));
        assertEquals("v1", vertices.get(2).getId());
        assertEquals("v2", vertices.get(3).getId());

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(namePropertyName, SortDirection.ASCENDING)
                .sort(agePropertyName, SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, "v2", "v1", "v3", "v4");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(namePropertyName, SortDirection.ASCENDING)
                .sort(agePropertyName, SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, "v2", "v1", "v4", "v3");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .vertices());
        assertVertexIds(vertices, "v1", "v2", "v3", "v4");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.DESCENDING)
                .vertices());
        assertVertexIds(vertices, "v4", "v3", "v2", "v1");

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort("otherfield", SortDirection.ASCENDING)
                .vertices());
        Assert.assertEquals(4, count(vertices));

        List<Edge> edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.LABEL_PROPERTY_NAME, SortDirection.ASCENDING)
                .edges());
        assertEdgeIds(edges, "e2", "e1", "e3");

        edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.LABEL_PROPERTY_NAME, SortDirection.DESCENDING)
                .edges());
        assertEdgeIds(edges, "e3", "e1", "e2");

        edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.OUT_VERTEX_ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .edges());
        assertEdgeIds(edges, "e1", "e2", "e3");

        edges = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(Edge.IN_VERTEX_ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING)
                .edges());
        assertEdgeIds(edges, "e1", "e2", "e3");

        graph.prepareVertex("v5", VISIBILITY_A)
                .setProperty(genderPropertyName, "female", VISIBILITY_A)
                .addExtendedData("table1", "row1", "column1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v6", VISIBILITY_A)
                .setProperty(genderPropertyName, "male", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = toList(graph.query(AUTHORIZATIONS_A_AND_B)
                .sort(genderPropertyName, SortDirection.ASCENDING)
                .vertices());
        Assert.assertEquals(6, count(vertices));
        assertEquals("v5", vertices.get(0).getId());
        assertEquals("v6", vertices.get(1).getId());
        assertTrue(vertices.get(2).getId().equals("v2") || vertices.get(2).getId().equals("v1"));
        assertTrue(vertices.get(3).getId().equals("v2") || vertices.get(3).getId().equals("v1"));
        assertTrue(vertices.get(4).getId().equals("v3") || vertices.get(4).getId().equals("v4"));
        assertTrue(vertices.get(5).getId().equals("v3") || vertices.get(5).getId().equals("v4"));
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
        assertVertexIds(vertices, "v2", "v1", "v3");

        vertices = graph.query("3", AUTHORIZATIONS_A_AND_B).vertices();
        assertVertexIds(vertices, "v3");

        vertices = graph.query("*", AUTHORIZATIONS_A_AND_B)
                .has("name", Compare.EQUAL, "3-1")
                .vertices();
        assertVertexIds(vertices, "v3");
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
    public void testGraphQueryTextVertexDifferentAuths() {
        graph.defineProperty("title").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();
        graph.defineProperty("fullText").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("title", "hello", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_B)
                .setProperty("fullText", StreamingPropertyValue.create("this is text with hello"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query("hello", AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, 1, (QueryResultsIterable) vertices);

        vertices = graph.query("hello", AUTHORIZATIONS_A_AND_B).vertices();
        assertResultsCount(2, 2, (QueryResultsIterable) vertices);
    }

    @Test
    public void testGraphQueryVertexDifferentAuths() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_B)
                .setProperty("age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        assertResultsCount(1, 1, (QueryResultsIterable) vertices);

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        assertResultsCount(2, 2, (QueryResultsIterable) vertices);
    }

    @Test
    public void testGraphQueryHidden() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e1", "v1", "v2", "junit edge", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e2 = graph.addEdge("e2", "v2", "v3", "junit edge", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        graph.markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.markVertexHidden(v1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices(FetchHints.ALL);
        assertResultsCount(2, vertices);
        assertVertexIdsAnyOrder(vertices, v2.getId(), v3.getId());

        QueryResultsIterable<String> vertexIds = graph.query(AUTHORIZATIONS_A).vertexIds(IdFetchHint.NONE);
        assertResultsCount(2, 2, vertexIds);
        assertIdsAnyOrder(vertexIds, v2.getId(), v3.getId());

        vertexIds = graph.query(AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(2, 2, vertexIds);
        assertIdsAnyOrder(vertexIds, v2.getId(), v3.getId());

        vertices = graph.query(AUTHORIZATIONS_A).vertices(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(3, vertices);
        assertVertexIdsAnyOrder(vertices, v1.getId(), v2.getId(), v3.getId());

        vertexIds = graph.query(AUTHORIZATIONS_A).vertexIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(3, 3, vertexIds);
        assertIdsAnyOrder(vertexIds, v1.getId(), v2.getId(), v3.getId());

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A).edges(FetchHints.ALL);
        assertResultsCount(1, edges);
        assertEdgeIdsAnyOrder(edges, e2.getId());

        QueryResultsIterable<String> edgeIds = graph.query(AUTHORIZATIONS_A).edgeIds(IdFetchHint.NONE);
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e2.getId());

        edgeIds = graph.query(AUTHORIZATIONS_A).edgeIds();
        assertResultsCount(1, 1, edgeIds);
        assertIdsAnyOrder(edgeIds, e2.getId());

        edges = graph.query(AUTHORIZATIONS_A).edges(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, edges);
        assertEdgeIdsAnyOrder(edges, e1.getId(), e2.getId());

        edgeIds = graph.query(AUTHORIZATIONS_A).edgeIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(2, 2, edgeIds);
        assertIdsAnyOrder(edgeIds, e1.getId(), e2.getId());

        QueryResultsIterable<Element> elements = graph.query(AUTHORIZATIONS_A).elements(FetchHints.ALL);
        assertResultsCount(3, elements);
        assertElementIdsAnyOrder(elements, v2.getId(), v3.getId(), e2.getId());

        QueryResultsIterable<String> elementIds = graph.query(AUTHORIZATIONS_A).elementIds(IdFetchHint.NONE);
        assertResultsCount(3, 3, elementIds);
        assertIdsAnyOrder(elementIds, v2.getId(), v3.getId(), e2.getId());

        elementIds = graph.query(AUTHORIZATIONS_A).elementIds();
        assertResultsCount(3, 3, elementIds);
        assertIdsAnyOrder(elementIds, v2.getId(), v3.getId(), e2.getId());

        elements = graph.query(AUTHORIZATIONS_A).elements(FetchHints.ALL_INCLUDING_HIDDEN);
        assertResultsCount(5, elements);
        assertElementIdsAnyOrder(elements, v1.getId(), v2.getId(), v3.getId(), e1.getId(), e2.getId());

        elementIds = graph.query(AUTHORIZATIONS_A).elementIds(IdFetchHint.ALL_INCLUDING_HIDDEN);
        assertResultsCount(5, 5, elementIds);
        assertIdsAnyOrder(elementIds, v1.getId(), v2.getId(), v3.getId(), e1.getId(), e2.getId());
    }

    @Test
    public void testGraphQueryVertexWithVisibilityChange() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, v1.getId());

        // change to same visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1 = v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();
        Assert.assertEquals(VISIBILITY_EMPTY, v1.getVisibility());

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(1, vertices);
        assertVertexIds(vertices, v1.getId());

        // change to new visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, vertices);
        assertEquals(0, count(vertices));
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

        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", v1, v3, LABEL_LABEL1, VISIBILITY_A)
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
        graph.addEdge("v2tov3", v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.flush();

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
    public void testDisableEdgeIndexing() {
        assumeTrue("disabling indexing not supported", disableEdgeIndexing(graph));

        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);

        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
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

        QueryResultsIterable<Vertex> vertices = graph.query("Ferner", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query("joe", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(2, vertices.getTotalHits());
        Assert.assertEquals(2, count(vertices));

        if (isLuceneQueriesSupported()) {
            vertices = graph.query("joe AND ferner", AUTHORIZATIONS_A)
                    .vertices();
            Assert.assertEquals(1, vertices.getTotalHits());
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
        Assert.assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe")
                .has("name", TextPredicate.CONTAINS, "Ferner")
                .vertices();
        Assert.assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe Ferner")
                .vertices();
        Assert.assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("propWithNonAlphaCharacters", TextPredicate.CONTAINS, "hyphen-word, etc.")
                .vertices();
        Assert.assertEquals(1, vertices.getTotalHits());
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

        assumeTrue("lucene and queries not supported", isLuceneQueriesSupported() && isLuceneAndQueriesSupported());

        Iterable<Vertex> vertices = graph.query("Joe AND ferner", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryHasWithSpacesAndFieldedQueryString() {
        assumeTrue("fielded query not supported", isFieldNamesInQuerySupported());

        graph.defineProperty("http://vertexium.org#name").dataType(String.class).textIndexHint(TextIndexHint.ALL).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("http://vertexium.org#name", "Joe Ferner", VISIBILITY_A)
                .setProperty("propWithHyphen", "hyphen-word", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("http://vertexium.org#name", "Joe Smith", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        assumeTrue("lucene queries", isLuceneQueriesSupported());

        Iterable<Vertex> vertices = graph.query("http\\:\\/\\/vertexium.org#name:Joe", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(2, count(vertices));

        vertices = graph.query("http\\:\\/\\/vertexium.org#name:\"Joe Ferner\"", AUTHORIZATIONS_A)
                .vertices();
        Assert.assertEquals(1, count(vertices));
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

        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        v3.prepareMutation()
                .setProperty("location", new GeoPoint(39.0299, -77.5121, "Ashburn, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoCircle(39.0299, -77.5121, 1))
                .vertices());
        Assert.assertEquals(1, count(vertices));
        geoPoint = (GeoPoint) vertices.get(0).getPropertyValue("location");
        assertEquals(39.0299, geoPoint.getLatitude(), 0.001);
        assertEquals(-77.5121, geoPoint.getLongitude(), 0.001);
        assertEquals("Ashburn, VA", geoPoint.getDescription());

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
        Assert.assertEquals(3, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoHash(38.9186, -77.2297, 3))
                .vertices());
        Assert.assertEquals(1, count(vertices));

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", TextPredicate.CONTAINS, "Reston")
                .vertices());
        Assert.assertEquals(2, count(vertices));
    }

    protected boolean isAdvancedGeoQuerySupported() {
        return true;
    }

    @Test
    public void testStoreGeoCircle() {
        assumeTrue("GeoCircle storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoCircle within = new GeoCircle(38.6270, -90.1994, 100, "St. Louis, MO - within");
        GeoCircle contains = new GeoCircle(38.6270, -90.1994, 800, "St. Louis, MO - contains");
        GeoCircle intersects = new GeoCircle(38.6270, -80.0, 500, "St. Louis, MO - intersects");
        GeoCircle disjoint = new GeoCircle(38.6270, -70.0, 500, "St. Louis, MO - disjoint");

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", within.getDescription()).vertices();
        assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        GeoCircle geoCircle = (GeoCircle) toList(vertices).get(0).getPropertyValue("location");
        assertEquals(within, geoCircle);
        assertEquals(within.getDescription(), geoCircle.getDescription());
    }

    @Test
    public void testStoreGeoRect() {
        assumeTrue("GeoRect storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoRect within = new GeoRect(new GeoPoint(39.52632, -91.35059), new GeoPoint(37.72767, -89.0482), "St. Louis, MO - within");
        GeoRect contains = new GeoRect(new GeoPoint(45.82157, -99.42435), new GeoPoint(31.43242, -80.97444), "St. Louis, MO - contains");
        GeoRect intersects = new GeoRect(new GeoPoint(43.1236, -85.75962), new GeoPoint(34.13039, -74.24038), "St. Louis, MO - intersects");
        GeoRect disjoint = new GeoRect(new GeoPoint(43.1236, -75.75962), new GeoPoint(34.13039, -64.24038), "St. Louis, MO - disjoint");

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", within.getDescription()).vertices();
        assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        GeoRect geoRect = (GeoRect) toList(vertices).get(0).getPropertyValue("location");
        assertEquals(within, geoRect);
        assertEquals(within.getDescription(), geoRect.getDescription());
    }

    @Test
    public void testStoreGeoLine() {
        assumeTrue("GeoLine storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoLine within = new GeoLine(new GeoPoint(39.5, -90.1994), new GeoPoint(37.9, -90.1994), "St. Louis, MO - within");
        GeoLine contains = new GeoLine(new GeoPoint(35.0, -100.0), new GeoPoint(39.5, -80), "St. Louis, MO - contains");
        GeoLine intersects = new GeoLine(new GeoPoint(38.67, -85), new GeoPoint(38.67, -80), "St. Louis, MO - intersects");
        GeoLine disjoint = new GeoLine(new GeoPoint(38.6, -74.0), new GeoPoint(38.6, -68), "St. Louis, MO - disjoint");

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", within.getDescription()).vertices();
        assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        GeoLine geoLine = (GeoLine) toList(vertices).get(0).getPropertyValue("location");
        assertEquals(within, geoLine);
        assertEquals(within.getDescription(), geoLine.getDescription());
    }

    @Test
    public void testStoreGeoPolygon() {
        assumeTrue("GeoPolygon storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoPolygon within = new GeoPolygon(Arrays.asList(new GeoPoint(39.4, -91.0), new GeoPoint(38.1, -91.0), new GeoPoint(38.627, -89.0), new GeoPoint(39.4, -91.0)), "St. Louis, MO - within");
        GeoPolygon contains = new GeoPolygon(Arrays.asList(new GeoPoint(50.0, -98.0), new GeoPoint(26.0, -98.0), new GeoPoint(38.627, -75.0), new GeoPoint(50.0, -98.0)), "St. Louis, MO - contains");
        GeoPolygon intersects = new GeoPolygon(Arrays.asList(new GeoPoint(43.0, -86.0), new GeoPoint(34.0, -86.0), new GeoPoint(38.627, -74.0), new GeoPoint(43.0, -86.0)), "St. Louis, MO - intersects");
        GeoPolygon disjoint = new GeoPolygon(Arrays.asList(new GeoPoint(43.0, -75.0), new GeoPoint(34.0, -75.0), new GeoPoint(38.627, -65.0), new GeoPoint(43.0, -75.0)), "St. Louis, MO - disjoint");

        // put a hole in the within triangle to make sure it gets stored/retrieved properly
        within.addHole(Arrays.asList(new GeoPoint(39.0, -90.5), new GeoPoint(38.627, -89.5), new GeoPoint(38.5, -90.5), new GeoPoint(39.0, -90.5)));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", within.getDescription()).vertices();
        assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        GeoPolygon geoPolygon = (GeoPolygon) toList(vertices).get(0).getPropertyValue("location");
        assertEquals(within, geoPolygon);
        assertEquals(within.getDescription(), geoPolygon.getDescription());
    }

    @Test
    public void testStoreGeoCollection() {
        assumeTrue("GeoCollection storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoCollection within = new GeoCollection("St. Louis, MO - within").addShape(new GeoCircle(38.6270, -90.1994, 100));
        GeoCollection contains = new GeoCollection("St. Louis, MO - contains").addShape(new GeoCircle(38.6270, -90.1994, 800));
        GeoCollection intersects = new GeoCollection("St. Louis, MO - intersects").addShape(new GeoCircle(38.6270, -80.0, 500));
        GeoCollection disjoint = new GeoCollection("St. Louis, MO - disjoint").addShape(new GeoCircle(38.6270, -70.0, 500));

        // Add another shape to within to make sure it stores/retrieves properly
        within.addShape(new GeoPoint(38.6270, -90.1994));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", within.getDescription()).vertices();
        assertEquals(1, vertices.getTotalHits());
        Assert.assertEquals(1, count(vertices));

        GeoCollection geoCollection = (GeoCollection) toList(vertices).get(0).getPropertyValue("location");
        assertEquals(within.getGeoShapes(), geoCollection.getGeoShapes());
        assertEquals(within.getDescription(), geoCollection.getDescription());
    }

    // See https://jsfiddle.net/mwizeman/do5ufpa9/ for a handy way to visualize the layout of the inputs and all of the search areas
    private void doALLGeoshapeTestQueries(GeoShape intersects, GeoShape disjoint, GeoShape within, GeoShape contains) {
        graph.defineProperty("location").dataType(GeoShape.class).define();
        graph.prepareVertex("v1", VISIBILITY_A).setProperty("location", intersects, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_A).setProperty("location", disjoint, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_A).setProperty("location", within, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_A).setProperty("location", contains, VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // All of the different search areas to try
        GeoCircle circle = new GeoCircle(38.6270, -90.1994, 500, "Circle");
        GeoRect rect = new GeoRect(new GeoPoint(43.1236, -95.9590), new GeoPoint(34.1303, -84.4397), "Rect");
        GeoPolygon triangle = new GeoPolygon(Arrays.asList(new GeoPoint(43.1236, -95.9590), new GeoPoint(34.1303, -95.9590), new GeoPoint(38.6270, -84.4397), new GeoPoint(43.1236, -95.9590)), "Triangle");
        GeoLine line = new GeoLine(Arrays.asList(new GeoPoint(34.1303, -95.9590), new GeoPoint(43.1236, -84.4397), new GeoPoint(38.6270, -84.4397)), "Line");
        GeoCollection collection = new GeoCollection("Collection")
                .addShape(new GeoCircle(38.6270, -90.1994, 250))
                .addShape(new GeoLine(new GeoPoint(39.5, -84.0), new GeoPoint(38.5, -84.0)));

        Arrays.asList(circle, rect, triangle, line, collection).forEach(searchArea -> {
            QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.INTERSECTS, searchArea).vertices();
            assertEquals("Incorrect total hits match INTERSECTS for shape " + searchArea.getDescription(), 3, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v1", "v3", "v4");

            vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.DISJOINT, searchArea).vertices();
            assertEquals("Incorrect total hits match DISJOINT for shape " + searchArea.getDescription(), 1, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v2");

            if (searchArea != line) {
                vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.WITHIN, searchArea).vertices();
                assertEquals("Incorrect total hits match WITHIN for shape " + searchArea.getDescription(), 1, vertices.getTotalHits());
                assertVertexIdsAnyOrder(vertices, "v3");

                vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.CONTAINS, searchArea).vertices();
                if (intersects instanceof GeoLine) {
                    assertEquals("Incorrect total hits match CONTAINS for shape " + searchArea.getDescription(), 0, vertices.getTotalHits());
                } else {
                    assertEquals("Incorrect total hits match CONTAINS for shape " + searchArea.getDescription(), 1, vertices.getTotalHits());
                    assertVertexIdsAnyOrder(vertices, "v4");
                }
            }
        });

        // Punch a hole in the polygon around the "within" shape and make sure that the results look ok
        triangle.addHole(Arrays.asList(new GeoPoint(40, -92.5), new GeoPoint(40, -88.5), new GeoPoint(37.4, -88.5), new GeoPoint(37.4, -92.5), new GeoPoint(40, -92.5)));
        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.INTERSECTS, triangle).vertices();
        assertEquals("Incorrect total hits match INTERSECTS for polygon with hole", 2, vertices.getTotalHits());
        assertVertexIdsAnyOrder(vertices, "v1", "v4");

        vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.DISJOINT, triangle).vertices();
        assertEquals("Incorrect total hits match DISJOINT for polygon with hole", 2, vertices.getTotalHits());
        assertVertexIdsAnyOrder(vertices, "v2", "v3");

        vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.WITHIN, triangle).vertices();
        assertEquals("Incorrect total hits match WITHIN for polygon with hole", 0, vertices.getTotalHits());

        vertices = graph.query(AUTHORIZATIONS_A_AND_B).has("location", GeoCompare.CONTAINS, triangle).vertices();
        if (intersects instanceof GeoLine) {
            assertEquals("Incorrect total hits match CONTAINS for polygon with hole", 0, vertices.getTotalHits());
        } else {
            assertEquals("Incorrect total hits match CONTAINS for polygon with hole", 1, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v4");
        }
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
        String propertyName = "prop.one";
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v1.setProperty(propertyName, "value1", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v2.setProperty(propertyName, "value2", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        v3.setProperty(propertyName, "value3", VISIBILITY_A, AUTHORIZATIONS_ALL);

        Edge ev1v2 = graph.prepareEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty(propertyName, "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge ev1v3 = graph.prepareEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A)
                .setProperty(propertyName, "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e v2->v3", v2, v3, LABEL_LABEL2, VISIBILITY_A)
                .setProperty(propertyName, "value2", VISIBILITY_A)
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
                .has(propertyName, "value2")
                .vertices();
        Assert.assertEquals(1, count(vertices));
        org.vertexium.test.util.IterableUtils.assertContains(v2, vertices);

        Iterable<Edge> edges = v1.query(AUTHORIZATIONS_A).edges();
        Assert.assertEquals(2, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1, LABEL_LABEL2).edges();
        Assert.assertEquals(2, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);
        org.vertexium.test.util.IterableUtils.assertContains(ev1v3, edges);

        edges = v1.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1).edges();
        Assert.assertEquals(1, count(edges));
        org.vertexium.test.util.IterableUtils.assertContains(ev1v2, edges);

        vertices = v1.query(AUTHORIZATIONS_A).hasEdgeLabel(LABEL_LABEL1).vertices();
        Assert.assertEquals(1, count(vertices));
        org.vertexium.test.util.IterableUtils.assertContains(v2, vertices);

        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.OUT).vertices(), "v2", "v3");
        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.IN).vertices());
        assertEdgeIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.OUT).edges(), "e v1->v2", "e v1->v3");
        assertEdgeIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasDirection(Direction.IN).edges());

        assertVertexIdsAnyOrder(v1.query(AUTHORIZATIONS_A).hasOtherVertexId("v2").vertices(), "v2");
        assertEdgeIds(v1.query(AUTHORIZATIONS_A).hasOtherVertexId("v2").edges(), "e v1->v2");
    }

    @Test
    public void testFindPaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v6 = graph.addVertex("v6", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v2
        graph.addEdge(v2, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v4
        graph.addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v3, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v5
        graph.addEdge(v4, v6, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v6
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v2", 2), AUTHORIZATIONS_A));
        List<Path> pathsByLabels = toList(graph.findPaths(new FindPathOptions("v1", "v2", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        List<Path> pathsByBadLabel = toList(graph.findPaths(new FindPathOptions("v1", "v2", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2")
        );

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v4", 2), AUTHORIZATIONS_A));
        pathsByLabels = toList(graph.findPaths(new FindPathOptions("v1", "v4", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toList(graph.findPaths(new FindPathOptions("v1", "v4", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2", "v4"),
                new Path("v1", "v3", "v4")
        );

        paths = toList(graph.findPaths(new FindPathOptions("v4", "v1", 2), AUTHORIZATIONS_A));
        pathsByLabels = toList(graph.findPaths(new FindPathOptions("v4", "v1", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toList(graph.findPaths(new FindPathOptions("v4", "v1", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v4", "v2", "v1"),
                new Path("v4", "v3", "v1")
        );

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v6", 3), AUTHORIZATIONS_A));
        pathsByLabels = toList(graph.findPaths(new FindPathOptions("v1", "v6", 3).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toList(graph.findPaths(new FindPathOptions("v1", "v6", 3).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2", "v4", "v6"),
                new Path("v1", "v3", "v4", "v6")
        );
    }

    @Test
    public void testFindPathExcludeLabels() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        graph.flush();

        assertPaths(
                graph.findPaths(new FindPathOptions("v1", "v4", 2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4"),
                new Path("v1", "v3", "v4")
        );

        assertPaths(
                graph.findPaths(new FindPathOptions("v1", "v4", 2).setExcludedLabels(LABEL_LABEL2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4")
        );
        assertPaths(
                graph.findPaths(new FindPathOptions("v1", "v4", 3).setExcludedLabels(LABEL_LABEL2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4")
        );
    }

    private void assertPaths(Iterable<Path> found, Path... expected) {
        List<Path> foundPaths = toList(found);
        List<Path> expectedPaths = new ArrayList<>();
        Collections.addAll(expectedPaths, expected);

        assertEquals(expectedPaths.size(), foundPaths.size());
        for (Path foundPath : foundPaths) {
            if (!expectedPaths.remove(foundPath)) {
                fail("Unexpected path: " + foundPath);
            }
        }
    }

    @Test
    public void testFindPathsWithSoftDeletedEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v1 -> v2
        Edge v2ToV3 = graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v2 -> v3
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3")
        );

        graph.softDeleteEdge(v2ToV3, AUTHORIZATIONS_A);
        graph.flush();

        assertNull(graph.getEdge(v2ToV3.getId(), AUTHORIZATIONS_A));
        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());
    }

    @Test
    public void testFindPathsWithHiddenEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v1 -> v2
        Edge v2ToV3 = graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v2 -> v3
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3")
        );

        graph.markEdgeHidden(v2ToV3, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        assertNull(graph.getEdge(v2ToV3.getId(), AUTHORIZATIONS_A_AND_B));
        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_B));
        assertEquals(1, paths.size());
    }

    @Test
    public void testFindPathsMultiplePaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v4
        graph.addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        graph.addEdge(v4, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        graph.addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v2", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2"),
                new Path("v1", "v3", "v2")
        );

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v2", 3), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2"),
                new Path("v1", "v3", "v2"),
                new Path("v1", "v3", "v4", "v2"),
                new Path("v1", "v4", "v3", "v2")
        );

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v5", 2), AUTHORIZATIONS_A));
        assertPaths(paths);

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v5", 3), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2", "v5"),
                new Path("v1", "v3", "v2", "v5")
        );
    }

    @Test
    public void testFindPathsWithDifferentVisibilityData() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A);

        graph.addEdge("v1v2", v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v1 -> v2
        graph.addEdge("v2v3", v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v2 -> v3
        graph.addEdge("v3v1", v3, v1, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v3 -> v1
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3"),
                new Path("v1", "v3")
        );

        graph.getEdge("v3v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        graph.getVertex("v2", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_EMPTY));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 3), AUTHORIZATIONS_EMPTY));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3"),
                new Path("v1", "v3")
        );

        graph.getVertex("v2", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(Visibility.EMPTY)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v2", 4), AUTHORIZATIONS_EMPTY));
        assertPaths(
                paths,
                new Path("v1", "v2")
        );
    }

    @Test
    public void testHasPath() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        graph.addEdge(v4, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        graph.addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5
        graph.flush();

        List<Path> paths = toList(graph.findPaths(new FindPathOptions("v1", "v4", 2, true), AUTHORIZATIONS_A));
        assertEquals(1, paths.size());

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v4", 3, true), AUTHORIZATIONS_A));
        assertEquals(1, paths.size());

        paths = toList(graph.findPaths(new FindPathOptions("v1", "v5", 2, true), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());
    }

    @Test
    public void testGetVerticesFromVertex() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
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
        graph.addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
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
    public void testElementMutationDoesntChangeObjectUntilSave() {
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
        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = graph.addEdge("e v2->v3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = graph.addEdge("e v3->v1", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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
        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = graph.addEdge("e v2->v3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = graph.addEdge("e v3->v1", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

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
        graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v2->v3", v2, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v1", v3, v1, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(4, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v3", LABEL_LABEL1, v1.getId(), v3.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v2->v3", LABEL_LABEL2, v2.getId(), v3.getId()), relatedEdges);
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v3->v1", LABEL_LABEL2, v3.getId(), v1.getId()), relatedEdges);
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterSoftDelete() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);

        graph.softDeleteEdge(e1, AUTHORIZATIONS_A);
        graph.flush();

        relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterMarkedHidden() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        org.vertexium.test.util.IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);

        graph.markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        relatedEdges = toList(graph.findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }

    @Test
    @Ignore // performance test
    @SuppressWarnings("unused")
    public void testFindRelatedEdgesPerformance() {
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
            graph.addEdge("e" + i, outVertex, inVertex, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
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
        graph.prepareVertex("v1", VISIBILITY_A)
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
    public void testMetadataMutationsOnVertex() {
        Metadata metadataPropB = new Metadata();
        metadataPropB.add("meta1", "meta1", VISIBILITY_A);
        Vertex vertex = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propBmeta", "propBmeta", metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", "meta2", VISIBILITY_A);
        vertex = m.save(AUTHORIZATIONS_ALL);

        assertEquals("meta2", vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
    }

    @Test
    public void testMetadataMutationsOnEdge() {
        Metadata metadataPropB = new Metadata();
        metadataPropB.add("meta1", "meta1", VISIBILITY_A);
        Edge edge = graph.prepareEdge("v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .setProperty("propBmeta", "propBmeta", metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        ExistingElementMutation<Edge> m = edge.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", "meta2", VISIBILITY_A);
        edge = m.save(AUTHORIZATIONS_ALL);

        assertEquals("meta2", edge.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
    }

    @Test
    public void testMetadataUpdate() {
        Metadata metadataPropB = new Metadata();
        metadataPropB.add("meta1", "value1", VISIBILITY_A);
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propBmeta", "propBmeta", metadataPropB, VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        FetchHints fetchHints = new FetchHintsBuilder()
                .setPropertyNamesToInclude("propBmeta")
                .build();
        Vertex vertex = graph.getVertex("v1", fetchHints, AUTHORIZATIONS_A);
        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        m.setPropertyMetadata("propBmeta", "meta1", "value2", VISIBILITY_A);
        m.save(AUTHORIZATIONS_ALL);
        graph.flush();

        vertex = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value2", vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());

        fetchHints = new FetchHintsBuilder()
                .setPropertyNamesToInclude("propBmeta")
                .build();
        vertex = graph.getVertex("v1", fetchHints, AUTHORIZATIONS_A);
        m = vertex.prepareMutation();
        Metadata newMetadata = new Metadata();
        newMetadata.add("meta1", "value3", VISIBILITY_A);
        m.setProperty("propBmeta", "propBmeta", newMetadata, VISIBILITY_A);
        m.save(AUTHORIZATIONS_ALL);
        graph.flush();

        vertex = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value3", vertex.getProperty("propBmeta").getMetadata().getEntry("meta1").getValue());
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
        assertVertexIdsAnyOrder(vertices, "v1", "v3", "v4");

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("exactMatch", "Test Value")
                .vertices();
        assertVertexIds(vertices, "v1");

        graph.query(AUTHORIZATIONS_A)
                .has("exactMatch", TextPredicate.DOES_NOT_CONTAIN, "Test")
                .vertices();

        graph.prepareVertex("v6", VISIBILITY_A)
                .setProperty("both", "susan-test", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        graph.prepareVertex("v7", VISIBILITY_A)
                .setProperty("both", "susan-test", Visibility.EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("both", TextPredicate.DOES_NOT_CONTAIN, "susan")
                .vertices();
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3", "v4", "v5");
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
        try {
            graph.query(AUTHORIZATIONS_A).has("none", TextPredicate.CONTAINS, "Test");
            fail("Full text queries should not be allowed for properties that are not indexed with FULL_TEXT.");
        } catch (VertexiumException ve) {
            assertEquals("Check your TextIndexHint settings. Property none is not full text indexed.", ve.getMessage());
        }
    }

    @Test
    public void testGetStreamingPropertyValueInputStreams() throws Exception {
        graph.defineProperty("a").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("b").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("c").dataType(String.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("a", StreamingPropertyValue.create("Test Value A"), VISIBILITY_A)
                .setProperty("b", StreamingPropertyValue.create("Test Value B"), VISIBILITY_A)
                .setProperty("c", StreamingPropertyValue.create("Test Value C"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        StreamingPropertyValue spvA = (StreamingPropertyValue) v1.getPropertyValue("a");
        assertEquals(12L, (long) spvA.getLength());
        StreamingPropertyValue spvB = (StreamingPropertyValue) v1.getPropertyValue("b");
        assertEquals(12L, (long) spvA.getLength());
        StreamingPropertyValue spvC = (StreamingPropertyValue) v1.getPropertyValue("c");
        assertEquals(12L, (long) spvA.getLength());
        ArrayList<StreamingPropertyValue> spvs = Lists.newArrayList(spvA, spvB, spvC);
        List<InputStream> streams = graph.getStreamingPropertyValueInputStreams(spvs);
        assertEquals("Test Value A", IOUtils.toString(streams.get(0)));
        assertEquals("Test Value B", IOUtils.toString(streams.get(1)));
        assertEquals("Test Value C", IOUtils.toString(streams.get(2)));
    }

    @Test
    public void testFieldBoost() throws Exception {
        assumeTrue("Boost not supported", graph.isFieldBoostSupported());

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

        assertVertexIds(graph.query("Test", AUTHORIZATIONS_A).vertices(), "v2", "v1");
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
    public void testValueTypesUpdatingWithMutations() throws Exception {
        Date date = createDate(2014, 2, 24, 13, 0, 5);

        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        graph.getVertex("v1", AUTHORIZATIONS_A_AND_B)
                .prepareMutation()
                .addPropertyValue("", "int", 5, VISIBILITY_A)
                .addPropertyValue("", "bigInteger", BigInteger.valueOf(10), VISIBILITY_A)
                .addPropertyValue("", "bigDecimal", BigDecimal.valueOf(1.1), VISIBILITY_A)
                .addPropertyValue("", "double", 5.6, VISIBILITY_A)
                .addPropertyValue("", "float", 6.4f, VISIBILITY_A)
                .addPropertyValue("", "string", "test", VISIBILITY_A)
                .addPropertyValue("", "byte", (byte) 5, VISIBILITY_A)
                .addPropertyValue("", "long", (long) 5, VISIBILITY_A)
                .addPropertyValue("", "boolean", true, VISIBILITY_A)
                .addPropertyValue("", "geopoint", new GeoPoint(77, -33), VISIBILITY_A)
                .addPropertyValue("", "short", (short) 5, VISIBILITY_A)
                .addPropertyValue("", "date", date, VISIBILITY_A)
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

        assertResultsCount(1, 1, graph.query(AUTHORIZATIONS_A).has("age", 25).vertices());

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_ALL);
        ExistingElementMutation<Vertex> m = v1.prepareMutation();
        m.alterElementVisibility(VISIBILITY_B);
        for (Property property : v1.getProperties()) {
            m.alterPropertyVisibility(property, VISIBILITY_B);
            m.setPropertyMetadata(property, "m1", "m1-value2", VISIBILITY_EMPTY);
        }
        m.save(AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_B);
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

        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_A).has("age", 25).vertices());
        assertResultsCount(1, 1, graph.query(AUTHORIZATIONS_B).has("age", 25).vertices());
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

        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .addPropertyValue("k2", "prop2", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterPropertyVisibility("k2", "prop2", VISIBILITY_B)
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

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));
        assertNotNull(v1.getProperty("prop2"));

        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_A).has("prop1", "value1").vertices());
        assertResultsCount(1, 1, graph.query(AUTHORIZATIONS_B).has("prop1", "value1").vertices());

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_A);
        if (propertyCountByValue != null) {
            assertEquals(null, propertyCountByValue.get("value1"));
        }

        propertyCountByValue = queryGraphQueryWithTermsAggregation("prop1", ElementType.VERTEX, AUTHORIZATIONS_B);
        if (propertyCountByValue != null) {
            assertEquals(1L, (long) propertyCountByValue.get("value1"));
        }

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        Property v1Prop1 = v1.getProperty("prop1");
        assertNotNull(v1Prop1);
        Assert.assertEquals(1, toList(v1Prop1.getMetadata().entrySet()).size());
        assertEquals("value1", v1Prop1.getMetadata().getValue("prop1_key1"));
        assertNotNull(v1.getProperty("prop2"));

        // alter and set property in one mutation
        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_B).has("prop1", "value1").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_A).has("prop1", "value1").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_B).has("prop1", "value1New").vertices());
        assertResultsCount(1, 1, graph.query(AUTHORIZATIONS_A).has("prop1", "value1New").vertices());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New", v1.getPropertyValue("prop1"));

        // alter visibility to the same visibility
        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New2", v1.getPropertyValue("prop1"));

        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_B).has("prop1", "value1").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_A).has("prop1", "value1").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_B).has("prop1", "value1New").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_A).has("prop1", "value1New").vertices());
        assertResultsCount(0, 0, graph.query(AUTHORIZATIONS_B).has("prop1", "value1New2").vertices());
        assertResultsCount(1, 1, graph.query(AUTHORIZATIONS_A).has("prop1", "value1New2").vertices());
    }

    @Test
    public void testAlterVisibilityAndSetMetadataInOneMutation() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("prop1_key1", "metadata1", VISIBILITY_EMPTY);

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .setPropertyMetadata("prop1", "prop1_key1", "metadata1New", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
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

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyVisibility(v1.getProperty("", "prop1", VISIBILITY_A), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getProperties()));
        assertNotNull(v1.getProperty("", "prop1", VISIBILITY_EMPTY));
        assertEquals("value2", v1.getProperty("", "prop1", VISIBILITY_EMPTY).getValue());
        assertNull(v1.getProperty("", "prop1", VISIBILITY_A));

        v1 = graph.getVertex("v1", graph.getDefaultFetchHints(), beforeAlterTimestamp, AUTHORIZATIONS_A);
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

        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
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
        PropertyValue propSmall = StreamingPropertyValue.create(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility("propSmall", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        Assert.assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
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

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop1", "prop1_key1", "valueNew", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().getEntry("prop1_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .setPropertyMetadata("prop2", "prop2_key1", "valueNew", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();
        assertEquals("valueNew", v1.getProperty("prop2").getMetadata().getEntry("prop2_key1", VISIBILITY_EMPTY).getValue());

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
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

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        Property p1 = v1.getProperty("prop1", VISIBILITY_A);
        v1.prepareMutation()
                .alterPropertyVisibility(p1, VISIBILITY_B)
                .setPropertyMetadata(p1, "prop1_key1", "valueNew", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        assertEquals("valueNew", v1.getProperty("prop1", VISIBILITY_B).getMetadata().getEntry("prop1_key1", VISIBILITY_B).getValue());
    }

    @Test
    public void testMetadata() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ExistingElementMutation<Vertex> m = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B).prepareMutation();
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", "metadata-value1aa", VISIBILITY_A);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_A), "metadata1", "metadata-value1ab", VISIBILITY_B);
        m.setPropertyMetadata(v1.getProperty("prop1", VISIBILITY_B), "metadata1", "metadata-value1bb", VISIBILITY_B);
        m.save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);

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
        assertVertexIds(vertices, "v1");
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
        assertVertexIds(vertices, "v1");
    }

    @Test
    public void testPartialUpdateOfVertexPropertyKey() {
        // see https://github.com/visallo/vertexium/issues/141
        assumeTrue("Known bug in partial updates", isParitalUpdateOfVertexPropertyKeySupported());

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop", "value1", VISIBILITY_A)
                .addPropertyValue("key2", "prop", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1")
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value2")
                .vertices();
        assertVertexIds(vertices, "v1");

        graph.prepareVertex("v1", VISIBILITY_A)
                .addPropertyValue("key1", "prop", "value1New", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1New")
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value2")
                .vertices();
        assertVertexIds(vertices, "v1");

        vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop", "value1")
                .vertices();
        assertVertexIds(vertices);
    }

    protected boolean isParitalUpdateOfVertexPropertyKeySupported() {
        return true;
    }

    @Test
    public void testAddVertexWithoutIndexing() {
        assumeTrue("add vertex without indexing not supported", !isDefaultSearchIndex());

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .vertices();
        assertVertexIds(vertices);
    }

    @Test
    public void testAlterVertexWithoutIndexing() {
        assumeTrue("alter vertex without indexing not supported", !isDefaultSearchIndex());

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
        assertVertexIds(vertices);
    }

    @Test
    public void testAddEdgeWithoutIndexing() {
        assumeTrue("add edge without indexing not supported", !isDefaultSearchIndex());

        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A_AND_B)
                .has("prop1", "value1")
                .edges();
        assertEdgeIds(edges);
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
        assumeTrue("query similar", graph.isQuerySimilarToTextSupported());

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

        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
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
        graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, time30.getTime(), VISIBILITY_A)
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
        assertEquals(25, graph.getVertex("v1", graph.getDefaultFetchHints(), time25.getTime(), AUTHORIZATIONS_A).getPropertyValue("", "age"));
        assertNull("v3 should not exist at time25", graph.getVertex("v3", graph.getDefaultFetchHints(), time25.getTime(), AUTHORIZATIONS_A));
        assertEquals("e1 should not exist", 0, count(graph.getEdges(graph.getDefaultFetchHints(), time25.getTime(), AUTHORIZATIONS_A)));
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

        ElementMutation<Edge> edgeMutation = graph.prepareEdge(edgeId, v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY);
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
    public void testTimestampsInExistingElementMutation() {
        long t1 = createDate(2017, 1, 18, 9, 20, 0).getTime();
        long t2 = createDate(2017, 1, 19, 9, 20, 0).getTime();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "prop1", "test1", new Metadata(), t1, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_ALL);
        assertEquals(t1, v1.getProperty("k1", "prop1").getTimestamp());

        graph.getVertex("v1", AUTHORIZATIONS_ALL)
                .prepareMutation()
                .addPropertyValue("k1", "prop1", "test2", new Metadata(), t2, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_ALL);
        assertEquals(t2, v1.getProperty("k1", "prop1").getTimestamp());

        List<HistoricalPropertyValue> historicalValues = toList(v1.getHistoricalPropertyValues("k1", "prop1", VISIBILITY_EMPTY, AUTHORIZATIONS_ALL));
        assertEquals(2, historicalValues.size());
        assertEquals(t1, historicalValues.get(1).getTimestamp());
        assertEquals(t2, historicalValues.get(0).getTimestamp());
    }

    @Test
    public void testGraphQueryWithTermsAggregation() {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();

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
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e3", "v1", "v2", LABEL_LABEL2, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("name", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("Joseph"));

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("emptyField", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(0, vertexPropertyCountByValue.size());

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("name", ElementType.VERTEX, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joseph"));

        Map<Object, Long> edgePropertyCountByValue = queryGraphQueryWithTermsAggregation(Edge.LABEL_PROPERTY_NAME, ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", edgePropertyCountByValue != null);
        assertEquals(2, edgePropertyCountByValue.size());
        assertEquals(2L, (long) edgePropertyCountByValue.get(LABEL_LABEL1));
        assertEquals(1L, (long) edgePropertyCountByValue.get(LABEL_LABEL2));

        vertexPropertyCountByValue = queryGraphQueryWithTermsAggregation("Joe", "name", ElementType.VERTEX, AUTHORIZATIONS_EMPTY);
        assumeTrue("terms aggregation not supported", vertexPropertyCountByValue != null);
        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Joe"));
        assertEquals(searchIndexFieldLevelSecurity ? 1L : 2L, (long) vertexPropertyCountByValue.get("Joseph"));
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
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_A);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.VERTEX, AUTHORIZATIONS_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());
    }

    @Test
    public void testGraphQueryEdgeWithTermsAggregationAlterElementVisibility() {
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addPropertyValue("k1", "age", 25, VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A_AND_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_A);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(0, propertyCountByValue.size());

        propertyCountByValue = queryGraphQueryWithTermsAggregation("age", ElementType.EDGE, AUTHORIZATIONS_B);
        assumeTrue("terms aggregation not supported", propertyCountByValue != null);
        assertEquals(1, propertyCountByValue.size());
    }

    private Map<Object, Long> queryGraphQueryWithTermsAggregation(String propertyName, ElementType elementType, Authorizations authorizations) {
        return queryGraphQueryWithTermsAggregation(null, propertyName, elementType, authorizations);
    }

    private Map<Object, Long> queryGraphQueryWithTermsAggregation(String queryString, String propertyName, ElementType elementType, Authorizations authorizations) {
        Query q = (queryString == null ? graph.query(authorizations) : graph.query(queryString, authorizations)).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", propertyName);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", agg.getClass().getName());
            return null;
        }
        q.addAggregation(agg);
        QueryResultsIterable<? extends Element> elements = elementType == ElementType.VERTEX ? q.vertices() : q.edges();
        TermsResult aggregationResult = elements.getAggregationResult("terms-count", TermsResult.class);
        return termsBucketToMap(aggregationResult.getBuckets());
    }

    @Test
    public void testGraphQueryWithNestedTermsAggregation() {
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
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

        Query q = graph.query(AUTHORIZATIONS_A_AND_B).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.vertices().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test
    public void testVertexQueryWithNestedTermsAggregation() {
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("gender").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Joe", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "male", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "male", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "female", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v5", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "name", "Sam", VISIBILITY_EMPTY)
                .addPropertyValue("k1", "gender", "female", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.addEdge("v1", "v2", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("v1", "v3", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("v1", "v4", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.addEdge("v1", "v5", LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Query q = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).query(AUTHORIZATIONS_A_AND_B).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.vertices().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test
    public void testVertexQueryWithNestedTermsAggregationOnExtendedData() {
        graph.defineProperty("name").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH, TextIndexHint.FULL_TEXT).define();
        graph.defineProperty("gender").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();

        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r1", "name", "Joe", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r1", "gender", "male", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r2", "name", "Sam", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r2", "gender", "male", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r3", "name", "Sam", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r3", "gender", "female", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r4", "name", "Sam", VISIBILITY_EMPTY)
                .addExtendedData("t1", "r4", "gender", "female", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        Query q = v1.getExtendedData("t1").query(AUTHORIZATIONS_A_AND_B).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", "name");
        agg.addNestedAggregation(new TermsAggregation("nested", "gender"));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        TermsResult aggregationResult = q.extendedDataRows().getAggregationResult("terms-count", TermsResult.class);
        Map<Object, Map<Object, Long>> vertexPropertyCountByValue = nestedTermsBucketToMap(aggregationResult.getBuckets(), "nested");

        assertEquals(2, vertexPropertyCountByValue.size());
        assertEquals(1, vertexPropertyCountByValue.get("Joe").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Joe").get("male"));
        assertEquals(2, vertexPropertyCountByValue.get("Sam").size());
        assertEquals(1L, (long) vertexPropertyCountByValue.get("Sam").get("male"));
        assertEquals(2L, (long) vertexPropertyCountByValue.get("Sam").get("female"));
    }

    @Test
    public void testGraphQueryWithHistogramAggregation() throws ParseException {
        boolean searchIndexFieldLevelSecurity = isSearchIndexFieldLevelSecuritySupported();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        graph.defineProperty("emptyField").dataType(Integer.class).define();

        String agePropertyName = "age.property";
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 25, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1990-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 20, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-09-04"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 20, VISIBILITY_EMPTY)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-08-15"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 20, VISIBILITY_A)
                .addPropertyValue("", "birthDate", simpleDateFormat.parse("1995-03-02"), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Map<Object, Long> histogram = queryGraphQueryWithHistogramAggregation(agePropertyName, "1", 0L, new HistogramAggregation.ExtendedBounds<>(20L, 25L), AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(6, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("20"));

        histogram = queryGraphQueryWithHistogramAggregation(agePropertyName, "1", null, null, AUTHORIZATIONS_A_AND_B);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("25"));
        assertEquals(3L, (long) histogram.get("20"));

        // field that doesn't have any values
        histogram = queryGraphQueryWithHistogramAggregation("emptyField", "1", null, null, AUTHORIZATIONS_A_AND_B);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(0, histogram.size());

        // date by 'year'
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", "year", null, null, AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());

        // date by milliseconds
        histogram = queryGraphQueryWithHistogramAggregation("birthDate", (365L * 24L * 60L * 60L * 1000L) + "", null, null, AUTHORIZATIONS_EMPTY);
        assumeTrue("histogram aggregation not supported", histogram != null);
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
    public void testGraphQueryWithRangeAggregation() throws ParseException {
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

        // numeric range
        RangeResult aggregationResult = queryGraphQueryWithRangeAggregation(
                "age",
                null,
                "lower",
                21,
                "middle",
                23,
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(searchIndexFieldLevelSecurity ? 2 : 3, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(0, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // numeric range with permission to see more data
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "age",
                null,
                "lower",
                21,
                "middle",
                23,
                "upper",
                AUTHORIZATIONS_A_AND_B
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(3, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(0, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // range for a field with no values
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "emptyField",
                null,
                "lower",
                21,
                "middle",
                23,
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(0, IterableUtils.count(aggregationResult.getBuckets()));

        // date range with dates specified as strings
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                null,
                "lower",
                "1991-01-01",
                "middle",
                "1995-08-30",
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());

        // date range without user specified keys
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                "yyyy-MM-dd",
                null,
                "1991-01-01",
                null,
                "1995-08-30",
                null,
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("*-1991-01-01").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("1991-01-01-1995-08-30").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("1995-08-30-*").getCount());

        // date range with dates specified as date objects
        aggregationResult = queryGraphQueryWithRangeAggregation(
                "birthDate",
                null,
                "lower",
                simpleDateFormat.parse("1991-01-01"),
                "middle",
                simpleDateFormat.parse("1995-08-30"),
                "upper",
                AUTHORIZATIONS_EMPTY
        );
        assumeTrue("range aggregation not supported", aggregationResult != null);
        assertEquals(1, aggregationResult.getBucketByKey("lower").getCount());
        assertEquals(2, aggregationResult.getBucketByKey("middle").getCount());
        assertEquals(1, aggregationResult.getBucketByKey("upper").getCount());
    }

    private RangeResult queryGraphQueryWithRangeAggregation(
            String propertyName,
            String format,
            String keyOne,
            Object boundaryOne,
            String keyTwo,
            Object boundaryTwo,
            String keyThree,
            Authorizations authorizations
    ) {
        Query q = graph.query(authorizations).limit(0);
        RangeAggregation agg = new RangeAggregation("range-count", propertyName, format);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", RangeAggregation.class.getName());
            return null;

        }
        agg.addUnboundedTo(keyOne, boundaryOne);
        agg.addRange(keyTwo, boundaryOne, boundaryTwo);
        agg.addUnboundedFrom(keyThree, boundaryTwo);
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("range-count", RangeResult.class);
    }

    @Test
    public void testGraphQueryWithRangeAggregationAndNestedTerms() throws ParseException {
        String agePropertyName = "age.property";
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 25, VISIBILITY_EMPTY)
                .addPropertyValue("", "name", "Alice", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 20, VISIBILITY_EMPTY)
                .addPropertyValue("", "name", "Alice", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v3", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 21, VISIBILITY_EMPTY)
                .addPropertyValue("", "name", "Alice", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.prepareVertex("v4", VISIBILITY_EMPTY)
                .addPropertyValue("", agePropertyName, 22, VISIBILITY_EMPTY)
                .addPropertyValue("", "name", "Bob", VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        Query q = graph.query(AUTHORIZATIONS_A_AND_B).limit(0);
        RangeAggregation rangeAggregation = new RangeAggregation("range-count", agePropertyName);
        TermsAggregation termsAggregation = new TermsAggregation("name-count", "name");
        rangeAggregation.addNestedAggregation(termsAggregation);

        assumeTrue("range aggregation not supported", q.isAggregationSupported(rangeAggregation));
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(termsAggregation));


        rangeAggregation.addUnboundedTo("lower", 23);
        rangeAggregation.addUnboundedFrom("upper", 23);
        q.addAggregation(rangeAggregation);

        RangeResult rangeAggResult = q.vertices().getAggregationResult("range-count", RangeResult.class);
        assertEquals(3, rangeAggResult.getBucketByKey("lower").getCount());
        assertEquals(1, rangeAggResult.getBucketByKey("upper").getCount());

        Comparator<TermsBucket> bucketComparator = (b1, b2) -> Long.compare(b2.getCount(), b1.getCount());

        Map<String, AggregationResult> lowerNestedResult = rangeAggResult.getBucketByKey("lower").getNestedResults();
        TermsResult lowerTermsResult = (TermsResult) lowerNestedResult.get(termsAggregation.getAggregationName());
        List<TermsBucket> lowerTermsBuckets = IterableUtils.toList(lowerTermsResult.getBuckets());
        Collections.sort(lowerTermsBuckets, bucketComparator);
        assertEquals(1, lowerNestedResult.size());
        assertEquals(2, lowerTermsBuckets.size());
        assertEquals("Alice", lowerTermsBuckets.get(0).getKey());
        assertEquals(2, lowerTermsBuckets.get(0).getCount());
        assertEquals("Bob", lowerTermsBuckets.get(1).getKey());
        assertEquals(1, lowerTermsBuckets.get(1).getCount());

        Map<String, AggregationResult> upperNestedResult = rangeAggResult.getBucketByKey("upper").getNestedResults();
        TermsResult upperTermsResult = (TermsResult) upperNestedResult.get(termsAggregation.getAggregationName());
        List<TermsBucket> upperTermsBuckets = IterableUtils.toList(upperTermsResult.getBuckets());
        assertEquals(1, upperNestedResult.size());
        assertEquals(1, upperTermsBuckets.size());
        assertEquals("Alice", upperTermsBuckets.get(0).getKey());
        assertEquals(1, upperTermsBuckets.get(0).getCount());
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
        assumeTrue("statistics aggregation not supported", stats != null);
        assertEquals(3, stats.getCount());
        assertEquals(65.0, stats.getSum(), 0.1);
        assertEquals(20.0, stats.getMin(), 0.1);
        assertEquals(25.0, stats.getMax(), 0.1);
        assertEquals(2.35702, stats.getStandardDeviation(), 0.1);
        assertEquals(21.666666, stats.getAverage(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("emptyField", AUTHORIZATIONS_EMPTY);
        assumeTrue("statistics aggregation not supported", stats != null);
        assertEquals(0, stats.getCount());
        assertEquals(0.0, stats.getSum(), 0.1);
        assertEquals(0.0, stats.getMin(), 0.1);
        assertEquals(0.0, stats.getMax(), 0.1);
        assertEquals(0.0, stats.getAverage(), 0.1);
        assertEquals(0.0, stats.getStandardDeviation(), 0.1);

        stats = queryGraphQueryWithStatisticsAggregation("age", AUTHORIZATIONS_A_AND_B);
        assumeTrue("statistics aggregation not supported", stats != null);
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
    public void testGraphQueryWithPercentilesAggregation() throws ParseException {
        graph.defineProperty("emptyField").dataType(Integer.class).define();

        for (int i = 0; i <= 100; i++) {
            graph.prepareVertex("v" + i, VISIBILITY_EMPTY)
                    .addPropertyValue("", "age", i, VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_A_AND_B);
        }
        graph.prepareVertex("v200", VISIBILITY_EMPTY)
                .addPropertyValue("", "age", 30, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        PercentilesResult percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assumeTrue("percentiles aggregation not supported", percentilesResult != null);
        List<Percentile> percentiles = IterableUtils.toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        assertEquals(7, percentiles.size());
        assertEquals(1.0, percentiles.get(0).getPercentile(), 0.1);
        assertEquals(1.0, percentiles.get(0).getValue(), 0.1);
        assertEquals(5.0, percentiles.get(1).getPercentile(), 0.1);
        assertEquals(5.0, percentiles.get(1).getValue(), 0.1);
        assertEquals(25.0, percentiles.get(2).getPercentile(), 0.1);
        assertEquals(25.0, percentiles.get(2).getValue(), 0.1);
        assertEquals(50.0, percentiles.get(3).getPercentile(), 0.1);
        assertEquals(50.0, percentiles.get(3).getValue(), 0.1);
        assertEquals(75.0, percentiles.get(4).getPercentile(), 0.1);
        assertEquals(75.0, percentiles.get(4).getValue(), 0.1);
        assertEquals(95.0, percentiles.get(5).getPercentile(), 0.1);
        assertEquals(95.0, percentiles.get(5).getValue(), 0.1);
        assertEquals(99.0, percentiles.get(6).getPercentile(), 0.1);
        assertEquals(99.0, percentiles.get(6).getValue(), 0.1);

        percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, 60, 99.99);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = IterableUtils.toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        assertEquals(2, percentiles.size());
        assertEquals(60.0, percentiles.get(0).getValue(), 0.1);
        assertEquals(60.0, percentiles.get(0).getValue(), 0.1);
        assertEquals(99.99, percentiles.get(1).getValue(), 0.1);
        assertEquals(99.99, percentiles.get(1).getValue(), 0.1);

        percentilesResult = queryGraphQueryWithPercentilesAggregation("emptyField", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = IterableUtils.toList(percentilesResult.getPercentiles());
        assertEquals(0, percentiles.size());

        percentilesResult = queryGraphQueryWithPercentilesAggregation("age", VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        assumeTrue("statistics aggregation not supported", percentilesResult != null);
        percentiles = IterableUtils.toList(percentilesResult.getPercentiles());
        percentiles.sort(Comparator.comparing(Percentile::getPercentile));
        assertEquals(7, percentiles.size());
        assertEquals(1.0, percentiles.get(0).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(0).getValue(), 0.1);
        assertEquals(5.0, percentiles.get(1).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(1).getValue(), 0.1);
        assertEquals(25.0, percentiles.get(2).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(2).getValue(), 0.1);
        assertEquals(50.0, percentiles.get(3).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(3).getValue(), 0.1);
        assertEquals(75.0, percentiles.get(4).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(4).getValue(), 0.1);
        assertEquals(95.0, percentiles.get(5).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(5).getValue(), 0.1);
        assertEquals(99.0, percentiles.get(6).getPercentile(), 0.1);
        assertEquals(30.0, percentiles.get(6).getValue(), 0.1);
    }

    private PercentilesResult queryGraphQueryWithPercentilesAggregation(
            String propertyName,
            Visibility visibility,
            Authorizations authorizations,
            double... percents
    ) {
        Query q = graph.query(authorizations).limit(0);
        PercentilesAggregation agg = new PercentilesAggregation("percentiles", propertyName, visibility);
        agg.setPercents(percents);
        if (!q.isAggregationSupported(agg)) {
            LOGGER.warn("%s unsupported", StatisticsAggregation.class.getName());
            return null;
        }
        q.addAggregation(agg);
        return q.vertices().getAggregationResult("percentiles", PercentilesResult.class);
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
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(searchIndexFieldLevelSecurity ? 2L : 3L, (long) histogram.get("dq"));

        histogram = queryGraphQueryWithGeohashAggregation("emptyField", 2, AUTHORIZATIONS_EMPTY);
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(0, histogram.size());

        histogram = queryGraphQueryWithGeohashAggregation("location", 2, AUTHORIZATIONS_A_AND_B);
        assumeTrue("geo hash histogram aggregation not supported", histogram != null);
        assertEquals(2, histogram.size());
        assertEquals(1L, (long) histogram.get("gb"));
        assertEquals(3L, (long) histogram.get("dq"));
    }

    @Test
    public void testGraphQueryWithCalendarFieldAggregation() {
        String dateFieldName = "agg_date_field";
        graph.prepareVertex("v0", VISIBILITY_EMPTY)
                .addPropertyValue("", "other_field", createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v1", VISIBILITY_EMPTY)
                .addPropertyValue("", dateFieldName, createDate(2016, Calendar.APRIL, 27, 10, 18, 56), VISIBILITY_A)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v2", VISIBILITY_EMPTY)
                .addPropertyValue("", dateFieldName, createDate(2017, Calendar.MAY, 26, 10, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v3", VISIBILITY_A_AND_B)
                .addPropertyValue("", dateFieldName, createDate(2016, Calendar.APRIL, 27, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v4", VISIBILITY_A_AND_B)
                .addPropertyValue("", dateFieldName, createDate(2016, Calendar.APRIL, 24, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v5", VISIBILITY_A_AND_B)
                .addPropertyValue("", dateFieldName, createDate(2016, Calendar.APRIL, 25, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.prepareVertex("v6", VISIBILITY_A_AND_B)
                .addPropertyValue("", dateFieldName, createDate(2016, Calendar.APRIL, 30, 12, 18, 56), VISIBILITY_EMPTY)
                .save(AUTHORIZATIONS_ALL);
        graph.flush();

        // hour of day
        QueryResultsIterable<Vertex> results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.HOUR_OF_DAY))
                .limit(0)
                .vertices();

        HistogramResult aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(2, aggResult.getBucketByKey(10).getCount());
        assertEquals(4, aggResult.getBucketByKey(12).getCount());

        // day of week
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.DAY_OF_WEEK))
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
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.DAY_OF_MONTH))
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
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.MONTH))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(Calendar.APRIL).getCount());
        assertEquals(1, aggResult.getBucketByKey(Calendar.MAY).getCount());

        // year
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.YEAR))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        assertEquals(2, count(aggResult.getBuckets()));
        assertEquals(5, aggResult.getBucketByKey(2016).getCount());
        assertEquals(1, aggResult.getBucketByKey(2017).getCount());

        // week of year
        results = graph.query(AUTHORIZATIONS_ALL)
                .addAggregation(new CalendarFieldAggregation("agg1", dateFieldName, null, TimeZone.getDefault(), Calendar.WEEK_OF_YEAR))
                .limit(0)
                .vertices();

        aggResult = results.getAggregationResult("agg1", CalendarFieldAggregation.RESULT_CLASS);
        if (isPainlessDateMath()) {
            assertEquals(3, count(aggResult.getBuckets()));
            assertEquals(1, aggResult.getBucketByKey(16).getCount());
            assertEquals(4, aggResult.getBucketByKey(17).getCount());
            assertEquals(1, aggResult.getBucketByKey(21).getCount());
        } else {
            assertEquals(2, count(aggResult.getBuckets()));
            assertEquals(5, aggResult.getBucketByKey(18).getCount());
            assertEquals(1, aggResult.getBucketByKey(21).getCount());
        }
    }

    /**
     * This is to mitigate a difference in date math between the Joda/Groovy and Painless scripting languages.
     * The only known difference is when calculating the WEEK_OF_YEAR. Painless appears to begin the week
     * on Monday while Joda/Groovy appear to use Sunday.
     *
     * @return true if date math is performed using the painless scripting language
     */
    protected boolean isPainlessDateMath() {
        return false;
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
        graph.prepareEdge("e1", "v1", LABEL_LABEL1, VISIBILITY_EMPTY)
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
        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
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

        graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.EDGE_LABELS, AUTHORIZATIONS_ALL);
        List<String> edgeLabels = toList(v1.getEdgesSummary(AUTHORIZATIONS_ALL).getEdgeLabels());
        assertEquals(2, edgeLabels.size());
        assertTrue(LABEL_LABEL1 + " missing", edgeLabels.contains(LABEL_LABEL1));
        assertTrue(LABEL_LABEL2 + " missing", edgeLabels.contains(LABEL_LABEL2));
    }

    @Test
    public void testFetchHintsEdgesSummary() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        graph.addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.addEdge("e v1->v3", v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_ALL);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.EDGE_LABELS, AUTHORIZATIONS_ALL);
        EdgesSummary summary = v1.getEdgesSummary(AUTHORIZATIONS_ALL);
        assertEquals(2, summary.getEdgeLabels().size());
        assertTrue(LABEL_LABEL1 + " missing", summary.getEdgeLabels().contains(LABEL_LABEL1));
        assertTrue(LABEL_LABEL2 + " missing", summary.getEdgeLabels().contains(LABEL_LABEL2));
        assertEquals(2, summary.getOutEdgeLabels().size());
        assertEquals(0, summary.getInEdgeLabels().size());
        assertEquals(1, (int) summary.getOutEdgeCountsByLabels().get(LABEL_LABEL1));
        assertEquals(1, (int) summary.getOutEdgeCountsByLabels().get(LABEL_LABEL2));
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
        Edge e1 = graph.prepareEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        Edge e2 = graph.prepareEdge("e2", v1, v2, LABEL_LABEL1, VISIBILITY_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Edge e1Loaded = graph.getEdge("e1", AUTHORIZATIONS_A);

        assertEquals(e1Loaded.hashCode(), e1.hashCode());
        assertTrue(e1Loaded.equals(e1));

        assertNotEquals(e1Loaded.hashCode(), e2.hashCode());
        assertFalse(e1Loaded.equals(e2));
    }

    @Test
    public void testCaseSensitivityOfExactMatch() {
        graph.defineProperty("text").dataType(String.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("text", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .setProperty("text", "joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v3", VISIBILITY_A)
                .setProperty("text", "JOE", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v4", VISIBILITY_A)
                .setProperty("text", "Joe", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("text", Compare.EQUAL, "Joe")
                .addAggregation(new TermsAggregation("agg1", "text"))
                .vertices();
        assertVertexIdsAnyOrder(vertices, "v1", "v2", "v3", "v4");

        TermsResult agg = vertices.getAggregationResult("agg1", TermsResult.class);
        ArrayList<TermsBucket> buckets = Lists.newArrayList(agg.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("Joe", buckets.get(0).getKey());
        assertEquals(4L, buckets.get(0).getCount());
    }

    @Test
    public void testExtendedData() {
        Date date1 = new Date(1487083490000L);
        Date date2 = new Date(1487083480000L);
        Date date3 = new Date(1487083470000L);
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "date", date1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "date", date2, VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value2", VISIBILITY_A)
                .addExtendedData("table1", "row3", "date", date3, VISIBILITY_A)
                .addExtendedData("table1", "row3", "name", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        AtomicInteger rowCount = new AtomicInteger();
        AtomicInteger rowPropertyCount = new AtomicInteger();
        graph.visitElements(new DefaultGraphVisitor() {
            @Override
            public void visitExtendedDataRow(Element element, String tableName, ExtendedDataRow row) {
                rowCount.incrementAndGet();
            }

            @Override
            public void visitProperty(Element element, String tableName, ExtendedDataRow row, Property property) {
                rowPropertyCount.incrementAndGet();
            }
        }, AUTHORIZATIONS_A);
        assertEquals(3, rowCount.get());
        assertEquals(6, rowPropertyCount.get());

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(ImmutableSet.of("table1"), v1.getExtendedDataTableNames());
        Iterator<ExtendedDataRow> rows = v1.getExtendedData("table1").iterator();

        ExtendedDataRow row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals("value1", row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals("value2", row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date3, row.getPropertyValue("date"));
        assertEquals("value3", row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        row = graph.getExtendedData(new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row1"), AUTHORIZATIONS_A);
        assertEquals("row1", row.getId().getRowId());
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals("value1", row.getPropertyValue("name"));

        rows = graph.getExtendedData(
                Lists.newArrayList(
                        new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row1"),
                        new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row2")
                ),
                AUTHORIZATIONS_A
        ).iterator();

        row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals("value1", row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals("value2", row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        rows = graph.getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A).iterator();

        row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals("value1", row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals("value2", row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date3, row.getPropertyValue("date"));
        assertEquals("value3", row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .addExtendedData("table1", "row4", "name", "value4", VISIBILITY_A)
                .addExtendedData("table2", "row1", "name", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertTrue("table1 should exist", v1.getExtendedDataTableNames().contains("table1"));
        assertTrue("table2 should exist", v1.getExtendedDataTableNames().contains("table2"));

        List<ExtendedDataRow> rowsList = toList(v1.getExtendedData("table1"));
        assertEquals(4, rowsList.size());
        rowsList = toList(v1.getExtendedData("table2"));
        assertEquals(1, rowsList.size());
    }

    @Test
    public void testExtendedDataInRange() {
        graph.prepareVertex("a", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("aa", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("az", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("b", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value4", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("aa", "a", "aa", "edge1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value5", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<ExtendedDataRow> rows = toList(graph.getExtendedDataInRange(ElementType.VERTEX, new Range(null, "a"), AUTHORIZATIONS_A));
        assertEquals(0, rows.size());

        rows = toList(graph.getExtendedDataInRange(ElementType.VERTEX, new Range(null, "b"), AUTHORIZATIONS_A));
        assertEquals(3, rows.size());
        List<String> rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3");

        rows = toList(graph.getExtendedDataInRange(ElementType.VERTEX, new Range(null, "bb"), AUTHORIZATIONS_A));
        assertEquals(4, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3", "value4");

        rows = toList(graph.getExtendedDataInRange(ElementType.VERTEX, new Range("aa", "b"), AUTHORIZATIONS_A));
        assertEquals(2, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value2", "value3");

        rows = toList(graph.getExtendedDataInRange(ElementType.VERTEX, new Range(null, null), AUTHORIZATIONS_A));
        assertEquals(4, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3", "value4");

        rows = toList(graph.getExtendedDataInRange(ElementType.EDGE, new Range(null, "a"), AUTHORIZATIONS_A));
        assertEquals(0, rows.size());

        rows = toList(graph.getExtendedDataInRange(ElementType.EDGE, new Range(null, "b"), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");

        rows = toList(graph.getExtendedDataInRange(ElementType.EDGE, new Range("aa", "b"), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");

        rows = toList(graph.getExtendedDataInRange(ElementType.EDGE, new Range(null, null), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> row.getPropertyValue("name").toString()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");
    }

    @Test
    public void testExtendedDataDifferentValue() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(v1.getExtendedData("table1"));
        assertEquals("value1", rows.get(0).getPropertyValue("name"));

        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        rows = Lists.newArrayList(v1.getExtendedData("table1"));
        assertEquals("value2", rows.get(0).getPropertyValue("name"));
    }

    @Test
    public void testExtendedDataDeleteColumn() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        // delete with wrong visibility
        graph.prepareVertex("v1", VISIBILITY_A)
                .deleteExtendedData("table1", "row1", "name", VISIBILITY_B)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<ExtendedDataRow> rows = Lists.newArrayList(graph.getVertex("v1", AUTHORIZATIONS_A).getExtendedData("table1"));
        assertEquals(1, rows.size());
        QueryResultsIterable<? extends VertexiumObject> searchResults = graph.query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(1, searchResults.getTotalHits());

        // delete with correct visibility
        clearGraphEvents();
        graph.getVertex("v1", AUTHORIZATIONS_A).prepareMutation()
                .deleteExtendedData("table1", "row1", "name", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEvents(
                new DeleteExtendedDataEvent(graph, v1, "table1", "row1", "name", null)
        );

        if (v1.getExtendedDataTableNames().size() == 0) {
            assertEquals("table names", 0, v1.getExtendedDataTableNames().size());
        } else {
            assertEquals("extended data rows", 0, Lists.newArrayList(v1.getExtendedData("table1")).size());
        }
        searchResults = graph.query("value", AUTHORIZATIONS_A)
                .search();
        List<VertexiumObject> searchResultsList = toList(searchResults);
        assertEquals("search result items", 0, searchResultsList.size());
        assertEquals("total hits", 0, searchResults.getTotalHits());
    }

    @Test
    public void testExtendedDataDelete() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        graph.deleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<? extends VertexiumObject> searchResults = graph.query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(0, searchResults.getTotalHits());
    }

    @Test
    public void testExtendedDataQueryVertices() {
        Date date1 = new Date(1487083490000L);
        Date date2 = new Date(1487083480000L);
        String tableName = "table.one";
        String rowOneName = "row.one";
        String rowTwoName = "row.two";
        String dateColumnName = "date.column";
        String nameColumnName = "name.column";

        graph.defineProperty(dateColumnName).sortable(true).dataType(Date.class).define();
        graph.defineProperty(nameColumnName).sortable(true).textIndexHint(TextIndexHint.values()).dataType(String.class).define();

        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData(tableName, rowOneName, dateColumnName, date1, VISIBILITY_A)
                .addExtendedData(tableName, rowOneName, nameColumnName, "value 1", VISIBILITY_A)
                .addExtendedData(tableName, rowTwoName, dateColumnName, date2, VISIBILITY_A)
                .addExtendedData(tableName, rowTwoName, nameColumnName, "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        // Should not come back when finding vertices
        QueryResultsIterable<Vertex> queryResults = graph.query(AUTHORIZATIONS_A)
                .has(dateColumnName, date1)
                .sort(dateColumnName, SortDirection.ASCENDING)
                .vertices();
        assertEquals(0, queryResults.getTotalHits());

        QueryResultsIterable<? extends VertexiumObject> searchResults = graph.query(AUTHORIZATIONS_A)
                .has(dateColumnName, date1)
                .sort(dateColumnName, SortDirection.ASCENDING)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        List<? extends VertexiumObject> searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        ExtendedDataRow searchResult = (ExtendedDataRow) searchResultsList.get(0);
        assertEquals("v1", searchResult.getId().getElementId());
        assertEquals(rowOneName, searchResult.getId().getRowId());

        searchResults = graph.query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);

        searchResults = graph.query("value", AUTHORIZATIONS_A)
                .hasExtendedData(ElementType.VERTEX, "v1", tableName)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);

        searchResults = graph.query("value", AUTHORIZATIONS_A)
                .hasExtendedData(tableName)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);
    }

    @Test
    public void testExtendedDataVertexQuery() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .addExtendedData("table2", "row3", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table2", "row4", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row6", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        List<ExtendedDataRow> searchResultsList = toList(
                v1.query(AUTHORIZATIONS_A)
                        .extendedDataRows()
        );
        assertRowIdsAnyOrder(Lists.newArrayList("row3", "row4", "row5", "row6"), searchResultsList);

        QueryResultsIterable<ExtendedDataRow> rows = graph.query(AUTHORIZATIONS_A).hasId("v1").extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = graph.query(AUTHORIZATIONS_A).hasId("v1", "v2").extendedDataRows();
        assertResultsCount(4, 4, rows);

        searchResultsList = toList(
                v1.query(AUTHORIZATIONS_A)
                        .sort(ExtendedDataRow.TABLE_NAME, SortDirection.ASCENDING)
                        .sort(ExtendedDataRow.ROW_ID, SortDirection.ASCENDING)
                        .extendedDataRows()
        );
        assertRowIds(Lists.newArrayList("row5", "row6", "row3", "row4"), searchResultsList);
    }

    @Test
    public void testExtendedDataEdgeQuery() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A)
                .addExtendedData("table1", "row3", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row4", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row6", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        List<ExtendedDataRow> searchResultsList = toList(
                getGraph().query("*", AUTHORIZATIONS_A)
                        .hasExtendedData(ElementType.EDGE, e1.getId(), "table1")
                        .extendedDataRows()
        );
        assertRowIdsAnyOrder(Lists.newArrayList("row5", "row6"), searchResultsList);

        QueryResultsIterable<ExtendedDataRow> rows = graph.query(AUTHORIZATIONS_A).hasId("e1").extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = graph.query(AUTHORIZATIONS_A).hasId("v1", "e1").extendedDataRows();
        assertResultsCount(4, 4, rows);
    }

    @Test
    public void testExtendedDataQueryAfterDeleteForVertex() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<ExtendedDataRow> searchResultsList = toList(graph.query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        graph.deleteVertex("v1", AUTHORIZATIONS_A);
        graph.flush();

        searchResultsList = toList(graph.query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList(), searchResultsList);
    }

    @Test
    public void testExtendedDataQueryAfterDeleteForEdge() {
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        List<ExtendedDataRow> searchResultsList = toList(graph.query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        graph.deleteEdge("e1", AUTHORIZATIONS_A);
        graph.flush();

        searchResultsList = toList(graph.query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList(), searchResultsList);
    }

    @Test
    public void testExtendedDataQueryEdges() {
        Date date1 = new Date(1487083490000L);
        Date date2 = new Date(1487083480000L);
        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareVertex("v2", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "date", date1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", "value 1", VISIBILITY_A)
                .addExtendedData("table1", "row2", "date", date2, VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", "value 2", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        // Should not come back when finding edges
        QueryResultsIterable<Edge> queryResults = graph.query(AUTHORIZATIONS_A)
                .has("date", date1)
                .edges();
        assertEquals(0, queryResults.getTotalHits());

        QueryResultsIterable<? extends VertexiumObject> searchResults = graph.query(AUTHORIZATIONS_A)
                .has("date", date1)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        List<? extends VertexiumObject> searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        ExtendedDataRow searchResult = (ExtendedDataRow) searchResultsList.get(0);
        assertEquals("e1", searchResult.getId().getElementId());
        assertEquals("row1", searchResult.getId().getRowId());

        searchResults = graph.query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);
    }

    @Test
    public void testExtendedDataQueryWithMultiValue() {
        graph.prepareVertex("v1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "col1", "key1", "joe", VISIBILITY_A)
                .addExtendedData("table1", "row1", "col1", "key2", "bob", VISIBILITY_A)
                .addExtendedData("table1", "row2", "col1", "key1", "joe", VISIBILITY_A)
                .addExtendedData("table1", "row2", "col1", "key2", "jane", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        QueryableIterable<ExtendedDataRow> rows = v1.getExtendedData("table1");
        for (ExtendedDataRow row : rows) {
            if (row.getId().getRowId().equals("row1")) {
                assertEquals("joe", row.getPropertyValue("key1", "col1"));
                assertEquals("bob", row.getPropertyValue("key2", "col1"));
            } else if (row.getId().getRowId().equals("row2")) {
                assertEquals("joe", row.getPropertyValue("key1", "col1"));
                assertEquals("jane", row.getPropertyValue("key2", "col1"));
            } else {
                throw new VertexiumException("invalid row: " + row.getId());
            }
        }

        QueryResultsIterable<? extends VertexiumObject> searchResults = graph.query("joe", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        List<? extends VertexiumObject> searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        searchResults = graph.query("bob", AUTHORIZATIONS_A)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1"), searchResultsList);
    }

    @Test
    public void testFetchHintsExceptions() {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);
        prop1Metadata.add("metadata2", "metadata2Value", VISIBILITY_A);

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        FetchHints propertyFetchHints = FetchHints.builder()
                .setPropertyNamesToInclude("prop2")
                .build();
        Vertex v1WithOnlyProp2 = graph.getVertex("v1", propertyFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1WithOnlyProp2.getProperties());
        assertThrowsException(() -> v1WithOnlyProp2.getProperty("prop1"));

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1WithAllProperties = graph.getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(v1WithAllProperties.getProperty("prop1")::getMetadata);

        FetchHints metadataFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .setMetadataKeysToInclude("metadata1")
                .build();
        Vertex v1WithOnlyMetadata1 = graph.getVertex("v1", metadataFetchHints, AUTHORIZATIONS_A);
        Property prop1 = v1WithOnlyMetadata1.getProperty("prop1");
        assertNotNull(prop1.getMetadata());
        assertNotNull(v1WithOnlyMetadata1.getProperty("prop1").getMetadata().getEntry("metadata1"));
        assertThrowsException(() -> v1WithOnlyMetadata1.getProperty("prop1").getMetadata().getEntry("metadata2"));
    }

    @Test
    public void testFetchHints() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n1", "value1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e1 = graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        e1.addPropertyValue("k1", "n1", "value1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v2, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        Vertex v1_none = graph.getVertex("v1", FetchHints.NONE, AUTHORIZATIONS_A);
        assertNotNull(v1_none);
        assertThrowsException(v1_none::getProperties);
        assertThrowsException(() -> v1_none.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_none.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        v1 = graph.getVertex("v1", graph.getDefaultFetchHints(), AUTHORIZATIONS_A);
        assertNotNull(v1);
        assertEquals(1, IterableUtils.count(v1.getProperties()));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        FetchHints propertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1_withProperties = graph.getVertex("v1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withProperties);
        assertEquals(1, IterableUtils.count(v1_withProperties.getProperties()));
        assertThrowsException(() -> v1_withProperties.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_withProperties.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        Vertex v1_withEdgeRegs = graph.getVertex("v1", FetchHints.EDGE_REFS, AUTHORIZATIONS_A);
        assertNotNull(v1_withEdgeRegs);
        assertThrowsException(v1_withEdgeRegs::getProperties);
        assertEquals(1, IterableUtils.count(v1_withEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(1, IterableUtils.count(v1_withEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        FetchHints inEdgeRefFetchHints = FetchHints.builder()
                .setIncludeInEdgeRefs(true)
                .build();
        Vertex v1_withInEdgeRegs = graph.getVertex("v1", inEdgeRefFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withInEdgeRegs);
        assertThrowsException(v1_withInEdgeRegs::getProperties);
        assertEquals(1, IterableUtils.count(v1_withInEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertThrowsException(() -> v1_withInEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A));

        FetchHints outEdgeRefFetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();
        Vertex v1_withOutEdgeRegs = graph.getVertex("v1", outEdgeRefFetchHints, AUTHORIZATIONS_A);
        assertNotNull(v1_withOutEdgeRegs);
        assertThrowsException(v1_withOutEdgeRegs::getProperties);
        assertThrowsException(() -> v1_withOutEdgeRegs.getEdges(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(1, IterableUtils.count(v1_withOutEdgeRegs.getEdges(Direction.OUT, AUTHORIZATIONS_A)));

        Edge e1_none = graph.getEdge("e1", FetchHints.NONE, AUTHORIZATIONS_A);
        assertNotNull(e1_none);
        assertThrowsException(e1_none::getProperties);
        assertEquals("v1", e1_none.getVertexId(Direction.OUT));
        assertEquals("v2", e1_none.getVertexId(Direction.IN));

        Edge e1_default = graph.getEdge("e1", graph.getDefaultFetchHints(), AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1_default.getProperties()));
        assertEquals("v1", e1_default.getVertexId(Direction.OUT));
        assertEquals("v2", e1_default.getVertexId(Direction.IN));

        Edge e1_properties = graph.getEdge("e1", propertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals(1, IterableUtils.count(e1_properties.getProperties()));
        assertEquals("v1", e1_properties.getVertexId(Direction.OUT));
        assertEquals("v2", e1_properties.getVertexId(Direction.IN));
    }

    @Test
    public void testFetchHintsProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n1", "value1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n2", "value2", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.addPropertyValue("k1", "n3", "value3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        FetchHints specificPropertiesFetchHints = FetchHints.builder()
                .setPropertyNamesToInclude("n1", "n3")
                .build();
        Vertex v1WithoutN2 = graph.getVertex("v1", specificPropertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals("value1", v1WithoutN2.getPropertyValue("n1"));
        assertThrowsException(() -> v1WithoutN2.getProperty("n1").getMetadata());
        assertThrowsException(() -> v1WithoutN2.getPropertyValue("n2"));
        assertEquals("value3", v1WithoutN2.getPropertyValue("n3"));

        FetchHints noPropertiesFetchHints = FetchHints.NONE;
        Vertex v1WithNotProperties = graph.getVertex("v1", noPropertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(v1WithNotProperties::getProperties);
        assertThrowsException(() -> v1WithNotProperties.getProperty("n1"));

        FetchHints allPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .setPropertyNamesToInclude("n1", "n3")
                .build();
        v1 = graph.getVertex("v1", allPropertiesFetchHints, AUTHORIZATIONS_A);
        assertEquals("value1", v1.getPropertyValue("n1"));
        assertEquals("value2", v1.getPropertyValue("n2"));
        assertEquals("value3", v1.getPropertyValue("n3"));
    }

    @Test
    public void testFetchHintsPropertyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Metadata metadata = new Metadata();
        metadata.add("m1", "m1value", VISIBILITY_A);
        metadata.add("m2", "m2value", VISIBILITY_A);
        metadata.add("m3", "m3value", VISIBILITY_A);
        v1.addPropertyValue("k1", "n1", "value1", metadata, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        FetchHints specificPropertiesFetchHints = FetchHints.builder()
                .setMetadataKeysToInclude("m1", "m3")
                .build();
        v1 = graph.getVertex("v1", specificPropertiesFetchHints, AUTHORIZATIONS_A);
        Metadata n1WithoutM2 = v1.getProperty("n1").getMetadata();
        assertEquals("m1value", n1WithoutM2.getValue("m1"));
        assertThrowsException(() -> n1WithoutM2.getValue("m2"));
        assertEquals("m3value", n1WithoutM2.getValue("m3"));

        FetchHints noPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllProperties(true)
                .build();
        Vertex v1_noMetadata = graph.getVertex("v1", noPropertiesFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_noMetadata.getProperty("n1").getMetadata());

        FetchHints allPropertiesFetchHints = FetchHints.builder()
                .setIncludeAllPropertyMetadata(true)
                .setMetadataKeysToInclude("m1", "m3")
                .build();
        Vertex v1_withMetadata = graph.getVertex("v1", allPropertiesFetchHints, AUTHORIZATIONS_A);
        Metadata n1 = v1_withMetadata.getProperty("n1").getMetadata();
        assertEquals("m1value", n1.getValue("m1"));
        assertEquals("m2value", n1.getValue("m2"));
        assertEquals("m3value", n1.getValue("m3"));
    }

    @Test
    public void testFetchHintsEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e3", v1, v2, LABEL_LABEL3, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e4", v1, v2, LABEL_LABEL3, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        FetchHints specificEdgeLabelFetchHints = FetchHints.builder()
                .setEdgeLabelsOfEdgeRefsToInclude(LABEL_LABEL1, LABEL_LABEL3)
                .build();
        v1 = graph.getVertex("v1", specificEdgeLabelFetchHints, AUTHORIZATIONS_A);
        List<org.vertexium.EdgeInfo> edgeInfos = toList(v1.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e1")));
        assertFalse(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e2")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e3")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e4")));

        FetchHints noEdgeLabelsFetchHints = FetchHints.NONE;
        Vertex v1_noEdges = graph.getVertex("v1", noEdgeLabelsFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_noEdges.getEdges(Direction.BOTH, AUTHORIZATIONS_A));
        assertThrowsException(() -> v1_noEdges.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));

        FetchHints edgeLabelsAndCountsFetchHints = FetchHints.builder()
                .setIncludeEdgeLabelsAndCounts(true)
                .build();
        Vertex v1_withEdgeLabelsAndCounts = graph.getVertex("v1", edgeLabelsAndCountsFetchHints, AUTHORIZATIONS_A);
        assertThrowsException(() -> v1_withEdgeLabelsAndCounts.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertEquals(
                LABEL_LABEL1 + "," + LABEL_LABEL2 + "," + LABEL_LABEL3,
                v1_withEdgeLabelsAndCounts.getEdgesSummary(AUTHORIZATIONS_A).getEdgeLabels().stream()
                        .sorted()
                        .collect(Collectors.joining(","))
        );

        FetchHints allEdgeInfoFetchHints = FetchHints.builder()
                .setIncludeAllEdgeRefs(true)
                .setEdgeLabelsOfEdgeRefsToInclude("m1", "m3")
                .build();
        Vertex v1_withEdgeRefs = graph.getVertex("v1", allEdgeInfoFetchHints, AUTHORIZATIONS_A);
        edgeInfos = toList(v1_withEdgeRefs.getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e1")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e2")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e3")));
        assertTrue(edgeInfos.stream().anyMatch(e -> e.getEdgeId().equals("e4")));
    }

    @Test
    public void benchmarkLotsOfProperties() {
        assumeTrue(benchmarkEnabled());

        int vertexCount = 100;
        int propertyNameCount = 10;
        int propertiesPerName = 50;
        int metadataPerProperty = 5;

        System.out.println("Defining properties");
        for (int i = 0; i < propertyNameCount; i++) {
            graph.defineProperty("prop" + i)
                    .textIndexHint(TextIndexHint.NONE)
                    .dataType(String.class)
                    .define();
        }

        System.out.println("Writing vertices");
        for (int vertexIndex = 0; vertexIndex < vertexCount; vertexIndex++) {
            String vertexId = "v" + vertexIndex;
            VertexBuilder m = graph.prepareVertex(vertexId, VISIBILITY_A);
            for (int propertyNameIndex = 0; propertyNameIndex < propertyNameCount; propertyNameIndex++) {
                for (int propertyPerNameIndex = 0; propertyPerNameIndex < propertiesPerName; propertyPerNameIndex++) {
                    Metadata metadata = new Metadata();
                    for (int metadataIndex = 0; metadataIndex < metadataPerProperty; metadataIndex++) {
                        metadata.add("m" + UUID.randomUUID().toString(), "value" + metadataIndex, VISIBILITY_A);
                    }
                    m.addPropertyValue("k" + UUID.randomUUID().toString(), "prop" + propertyNameIndex, "value" + propertyNameIndex, metadata, VISIBILITY_A);
                }
            }
            m.save(AUTHORIZATIONS_ALL);
        }
        graph.flush();

        System.out.println("Reading vertices");
        for (Vertex vertex : graph.getVertices(FetchHints.ALL, AUTHORIZATIONS_A)) {
            vertex.getId();
        }
    }

    @Test
    public void benchmark() {
        assumeTrue(benchmarkEnabled());
        Random random = new Random(1);
        int vertexCount = 10000;
        int edgeCount = 10000;
        int findVerticesByIdCount = 10000;

        benchmarkAddVertices(vertexCount);
        benchmarkAddEdges(random, vertexCount, edgeCount);
        benchmarkFindVerticesById(random, vertexCount, findVerticesByIdCount);
    }

    @Test
    public void benchmarkGetPropertyByName() {
        final int propertyCount = 100;

        assumeTrue(benchmarkEnabled());
        VertexBuilder m = graph.prepareVertex("v1", VISIBILITY_A);
        for (int i = 0; i < propertyCount; i++) {
            m.addPropertyValue("key", "prop" + i, "value " + i, VISIBILITY_A);
        }
        m.save(AUTHORIZATIONS_ALL);
        graph.flush();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_ALL);

        double startTime = System.currentTimeMillis();
        StringBuilder optimizationBuster = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            for (int propIndex = 0; propIndex < propertyCount; propIndex++) {
                Object value = v1.getPropertyValue("key", "prop" + propIndex);
                optimizationBuster.append(value.toString().substring(0, 1));
            }
        }
        double endTime = System.currentTimeMillis();
        LOGGER.trace("optimizationBuster: %s", optimizationBuster.substring(0, 1));
        LOGGER.info("get property by name and key in %.3fs", (endTime - startTime) / 1000);

        startTime = System.currentTimeMillis();
        optimizationBuster = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            for (int propIndex = 0; propIndex < propertyCount; propIndex++) {
                Object value = v1.getPropertyValue("prop" + propIndex);
                optimizationBuster.append(value.toString().substring(0, 1));
            }
        }
        endTime = System.currentTimeMillis();
        LOGGER.trace("optimizationBuster: %s", optimizationBuster.substring(0, 1));
        LOGGER.info("get property by name in %.3fs", (endTime - startTime) / 1000);
    }

    @Test
    public void benchmarkSaveElementMutations() {
        assumeTrue(benchmarkEnabled());
        int vertexCount = 1000;

        benchmarkAddVertices(vertexCount);
        benchmarkAddVerticesSaveElementMutations(vertexCount);
        benchmarkAddVertices(vertexCount);
    }

    private void benchmarkAddVertices(int vertexCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < vertexCount; i++) {
            String vertexId = "v" + i;
            graph.prepareVertex(vertexId, VISIBILITY_A)
                    .addPropertyValue("k1", "prop1", "value1 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", "value2 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", "value3 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", "value4 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", "value5 " + i, VISIBILITY_A)
                    .save(AUTHORIZATIONS_ALL);
        }
        graph.flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add vertices in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddVerticesSaveElementMutations(int vertexCount) {
        double startTime = System.currentTimeMillis();
        List<ElementMutation> mutations = new ArrayList<>();
        for (int i = 0; i < vertexCount; i++) {
            String vertexId = "v" + i;
            ElementBuilder<Vertex> m = graph.prepareVertex(vertexId, VISIBILITY_A)
                    .addPropertyValue("k1", "prop1", "value1 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", "value2 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", "value3 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", "value4 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", "value5 " + i, VISIBILITY_A);
            mutations.add(m);
        }
        graph.saveElementMutations(mutations, AUTHORIZATIONS_ALL);
        graph.flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("save element mutations in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkAddEdges(Random random, int vertexCount, int edgeCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < edgeCount; i++) {
            String edgeId = "e" + i;
            String outVertexId = "v" + random.nextInt(vertexCount);
            String inVertexId = "v" + random.nextInt(vertexCount);
            graph.prepareEdge(edgeId, outVertexId, inVertexId, LABEL_LABEL1, VISIBILITY_A)
                    .addPropertyValue("k1", "prop1", "value1 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop2", "value2 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop3", "value3 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop4", "value4 " + i, VISIBILITY_A)
                    .addPropertyValue("k1", "prop5", "value5 " + i, VISIBILITY_A)
                    .save(AUTHORIZATIONS_ALL);
        }
        graph.flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("add edges in %.3fs", (endTime - startTime) / 1000);
    }

    private void benchmarkFindVerticesById(Random random, int vertexCount, int findVerticesByIdCount) {
        double startTime = System.currentTimeMillis();
        for (int i = 0; i < findVerticesByIdCount; i++) {
            String vertexId = "v" + random.nextInt(vertexCount);
            graph.getVertex(vertexId, AUTHORIZATIONS_ALL);
        }
        graph.flush();
        double endTime = System.currentTimeMillis();
        LOGGER.info("find vertices by id in %.3fs", (endTime - startTime) / 1000);
    }

    private boolean benchmarkEnabled() {
        return Boolean.parseBoolean(System.getProperty("benchmark", "false"));
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
        Collections.sort(vertices, Comparator.comparing(Element::getId));
        return vertices;
    }

    protected boolean disableEdgeIndexing(Graph graph) {
        return false;
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

    // Historical Property Value tests
    @Test
    public void historicalPropertyValueAddProp() {

        Vertex vertexAdded = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Add property
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
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
        Vertex vertexAdded = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // remove property
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
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
    public void historicalPropertyValueModifyPropValue() {
        Vertex vertexAdded = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // modify property value
        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
                .setProperty("prop3_A", "value4", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Restore
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
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
        Vertex vertexAdded = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1_A", "value1", VISIBILITY_A)
                .setProperty("prop2_B", "value2", VISIBILITY_B)
                .setProperty("prop3_A", "value3", VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // modify property value
        Vertex v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
                .alterPropertyVisibility("prop1_A", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        // Restore
        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A_AND_B);
        vertexAdded = v1.prepareMutation()
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

    @Test
    @Ignore // elasticsearch takes a very long time to run this
    public void manyVisibilities() {
        int count = 1010; // 1000 is the default max for elasticsearch

        List<String> authsToAdd = new ArrayList<>();
        authsToAdd.add("all");
        for (int i = 0; i < count; i++) {
            String itemVisibilityString = String.format("v%d", i);
            authsToAdd.add(itemVisibilityString);
        }
        System.out.println("Adding auths");
        addAuthorizations(authsToAdd.toArray(new String[authsToAdd.size()]));

        System.out.println("Add vertices");
        for (int i = 0; i < count; i++) {
            String itemVisibilityString = String.format("v%d", i);
            Visibility visibility = new Visibility(String.format("v%d|all", i));
            Authorizations authorizations = createAuthorizations("all", itemVisibilityString);
            graph.prepareVertex(visibility)
                    .addPropertyValue("key1", "name1", itemVisibilityString, visibility)
                    .save(authorizations);
        }
        graph.flush();

        System.out.println("Getting vertices");
        Authorizations authorizations = createAuthorizations("all");
        ArrayList<Vertex> vertices = Lists.newArrayList(graph.getVertices(authorizations));
        assertEquals(count, vertices.size());

        System.out.println("Query vertices");
        vertices = Lists.newArrayList(graph.query(authorizations)
                .limit((Integer) null)
                .vertices());
        assertEquals(count, vertices.size());
    }
}
