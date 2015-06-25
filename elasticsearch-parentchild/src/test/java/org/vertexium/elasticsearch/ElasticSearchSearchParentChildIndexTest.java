package org.vertexium.elasticsearch;

import org.junit.*;
import org.vertexium.*;
import org.vertexium.*;
import org.vertexium.elasticsearch.helpers.ElasticSearchSearchParentChildIndexTestHelpers;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategyConfiguration;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.test.GraphTestBase;
import org.vertexium.type.GeoPoint;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticSearchSearchParentChildIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return ElasticSearchSearchParentChildIndexTestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        ElasticSearchSearchParentChildIndexTestHelpers.beforeClass();
    }

    @Before
    @Override
    public void before() throws Exception {
        ElasticSearchSearchParentChildIndexTestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        ElasticSearchSearchParentChildIndexTestHelpers.after();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        ElasticSearchSearchParentChildIndexTestHelpers.afterClass();
    }

    @Test
    public void testGeoPointLoadDefinition() {
        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        searchIndex.loadPropertyDefinitions();

        Map<String, PropertyDefinition> propertyDefinitions = searchIndex.getAllPropertyDefinitions();
        PropertyDefinition locationPropertyDef = propertyDefinitions.get("location");
        assertNotNull(locationPropertyDef);
        assertEquals(GeoPoint.class, locationPropertyDef.getDataType());
    }

    @Test
    public void testGetIndexRequests() throws IOException {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();

        String indexName = searchIndex.getIndexName(v1);
        IndexInfo indexInfo = searchIndex.ensureIndexCreatedAndInitialized(indexName, searchIndex.isStoreSourceData());
        assertNotNull(indexInfo);
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();
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
}
