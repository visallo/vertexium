package org.vertexium.elasticsearch;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.*;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.Metadata;
import org.vertexium.Vertex;
import org.vertexium.elasticsearch.helpers.ElasticSearchSearchIndexTestHelpers;
import org.vertexium.id.SimpleSubstitutionUtils;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.test.GraphTestBase;
import org.vertexium.test.util.LargeStringInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ElasticSearchSearchIndexSimpleSubstitutionTest extends GraphTestBase {
    private final String PROP1_PROPERTY_NAME = "prop1";
    private final String PROP1_SUBSTITUTION_NAME = "p1";
    private final String PROP_LARGE_PROPERTY_NAME = "propLarge";
    private final String PROP_LARGE_SUBSTITUTION_NAME = "pL";
    private final String PROP_SMALL_PROPERTY_NAME = "propSmall";
    private final String PROP_SMALL_SUBSTITUTION_NAME = "pS";

    @Override
    protected Graph createGraph() throws Exception {
        Map<String, String> substitutionMap = new HashMap<>();
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP1_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP1_SUBSTITUTION_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP_LARGE_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP_LARGE_SUBSTITUTION_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP_SMALL_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP_SMALL_SUBSTITUTION_NAME);
        return ElasticSearchSearchIndexTestHelpers.createGraphWithSubstitution(substitutionMap);
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        ElasticSearchSearchIndexTestHelpers.beforeClass();
    }

    @Before
    @Override
    public void before() throws Exception {
        ElasticSearchSearchIndexTestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        ElasticSearchSearchIndexTestHelpers.after();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        ElasticSearchSearchIndexTestHelpers.afterClass();
    }

    private ElasticSearchSearchIndex getSearchIndex() {
        return (ElasticSearchSearchIndex) ((InMemoryGraph) getGraph()).getSearchIndex();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    @Test
    public void testCreateJsonForElement() throws IOException {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty(PROP_SMALL_PROPERTY_NAME, propSmall, VISIBILITY_A)
                .setProperty(PROP_LARGE_PROPERTY_NAME, propLarge, VISIBILITY_A)
                .setProperty(PROP1_PROPERTY_NAME, "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        String jsonString = getSearchIndex().createJsonForElement(graph, v1, AUTHORIZATIONS_A_AND_B);
        JSONObject json = new JSONObject(jsonString);
        assertNotNull(json);
        assertFalse(jsonString.contains(PROP_LARGE_PROPERTY_NAME));
        assertFalse(jsonString.contains(PROP_SMALL_PROPERTY_NAME));
        assertFalse(jsonString.contains(PROP1_PROPERTY_NAME));
        assertTrue(jsonString.contains(PROP_SMALL_SUBSTITUTION_NAME));
        assertTrue(jsonString.contains(PROP_LARGE_SUBSTITUTION_NAME));
        assertTrue(jsonString.contains(PROP1_SUBSTITUTION_NAME));

        getSearchIndex().loadPropertyDefinitions();
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return false;
    }

    @Override
    protected boolean isIterableWithTotalHitsSupported(Iterable<Vertex> vertices) {
        return false;
    }
}
