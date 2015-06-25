package org.vertexium.elasticsearch;

import com.google.common.base.Joiner;
import org.junit.*;
import org.vertexium.*;
import org.vertexium.elasticsearch.helpers.ElasticSearchSearchParentChildIndexTestHelpers;
import org.vertexium.id.SimpleSubstitutionUtils;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.test.GraphTestBase;
import org.vertexium.type.GeoPoint;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSearchSearchParentChildIndexSimpleSubstitutionTest extends GraphTestBase {
    private final String PROP1_PROPERTY_NAME = "prop1";
    private final String PROP1_SUBSTITUTION_NAME = "p1";
    private final String PROP_LARGE_PROPERTY_NAME = "propLarge";
    private final String PROP_LARGE_SUBSTITUTION_NAME = "pL";
    private final String PROP_SMALL_PROPERTY_NAME = "propSmall";
    private final String PROP_SMALL_SUBSTITUTION_NAME = "pS";

    @Override
    protected Graph createGraph() throws Exception {
        Map<String, String> substitutionMap = new HashMap();
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP1_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP1_SUBSTITUTION_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP_LARGE_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP_LARGE_SUBSTITUTION_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.KEY_IDENTIFIER}), PROP_SMALL_PROPERTY_NAME);
        substitutionMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), PROP_SMALL_SUBSTITUTION_NAME);
        return ElasticSearchSearchParentChildIndexTestHelpers.createGraphWithSubstitution(substitutionMap);
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

    @Override
    protected boolean isEdgeBoostSupported() {
        return true;
    }

    private ElasticSearchParentChildSearchIndex getSearchIndex() {
        return (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();
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
        Assert.assertNotNull(locationPropertyDef);
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

        ElasticSearchParentChildSearchIndex searchIndex = getSearchIndex();

        String indexName = searchIndex.getIndexName(v1);
        IndexInfo indexInfo = searchIndex.ensureIndexCreatedAndInitialized(indexName, searchIndex.isStoreSourceData());
        assertTrue(indexInfo.isPropertyDefined("prop1"));
        Assert.assertNotNull(indexInfo);
    }
}
