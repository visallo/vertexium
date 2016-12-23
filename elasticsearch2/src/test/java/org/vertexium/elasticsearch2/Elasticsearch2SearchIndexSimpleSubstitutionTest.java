package org.vertexium.elasticsearch2;

import com.google.common.base.Joiner;
import org.vertexium.Graph;
import org.vertexium.id.SimpleSubstitutionUtils;

import java.util.HashMap;
import java.util.Map;

public class Elasticsearch2SearchIndexSimpleSubstitutionTest extends Elasticsearch2SearchIndexTestBase {
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
        return super.createGraph(substitutionMap);
    }
}
