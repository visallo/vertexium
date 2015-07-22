package org.vertexium.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.*;
import org.vertexium.*;
import org.vertexium.elasticsearch.helpers.ElasticSearchSearchIndexTestHelpers;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategyConfiguration;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.inmemory.InMemoryGraph;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.test.GraphTestBase;
import org.vertexium.test.util.LargeStringInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchSearchIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return ElasticSearchSearchIndexTestHelpers.createGraph();
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

    @Test
    public void testCreateJsonForElement() throws IOException {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        String jsonString = getSearchIndex().createJsonForElement(graph, v1, AUTHORIZATIONS_A_AND_B);
        JSONObject json = new JSONObject(jsonString);
        assertNotNull(json);

        getSearchIndex().loadPropertyDefinitions();
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        ElasticSearchSearchIndex searchIndex = getSearchIndex();
        ElasticSearchSearchIndexConfiguration config = searchIndex.getConfig();
        ScoringStrategy scoringStrategy = config.getScoringStrategy();
        if (!(scoringStrategy instanceof EdgeCountScoringStrategy)) {
            return false;
        }

        EdgeCountScoringStrategyConfiguration edgeCountScoringStrategyConfig = ((EdgeCountScoringStrategy) scoringStrategy).getConfig();

        try {
            Field updateEdgeBoostField = edgeCountScoringStrategyConfig.getClass().getDeclaredField("updateEdgeBoost");
            updateEdgeBoostField.setAccessible(true);
            updateEdgeBoostField.set(edgeCountScoringStrategyConfig, false);
        } catch (Exception e) {
            throw new VertexiumException("Failed to update 'updateEdgeBoost' field", e);
        }

        return true;
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return false;
    }
}
