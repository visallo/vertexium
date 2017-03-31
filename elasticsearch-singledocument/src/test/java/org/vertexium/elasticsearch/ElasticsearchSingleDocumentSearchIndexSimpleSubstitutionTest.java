package org.vertexium.elasticsearch;

import com.google.common.base.Joiner;
import org.junit.ClassRule;

import java.util.HashMap;

import static org.vertexium.id.SimpleSubstitutionUtils.*;

public class ElasticsearchSingleDocumentSearchIndexSimpleSubstitutionTest extends ElasticsearchSingleDocumentSearchIndexTestBase {
    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(new HashMap<String, String>() {{
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", KEY_IDENTIFIER}), "prop1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", VALUE_IDENTIFIER}), "p1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", KEY_IDENTIFIER}), "propLarge");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", VALUE_IDENTIFIER}), "pL");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", KEY_IDENTIFIER}), "propSmall");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", VALUE_IDENTIFIER}), "pS");
    }});

    @Override
    protected ElasticsearchResource getElasticsearchResource() {
        return elasticsearchResource;
    }
}
