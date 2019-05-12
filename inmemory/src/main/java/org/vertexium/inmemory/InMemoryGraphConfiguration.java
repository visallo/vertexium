package org.vertexium.inmemory;

import org.vertexium.GraphConfiguration;
import org.vertexium.inmemory.search.DefaultSearchIndex;

import java.util.Map;

public class InMemoryGraphConfiguration extends GraphConfiguration {
    public InMemoryGraphConfiguration(Map<String, Object> config) {
        super(update(config));
    }

    private static Map<String, Object> update(Map<String, Object> config) {
        if (!config.containsKey(SEARCH_INDEX_PROP_PREFIX)) {
            config.put(SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        }
        return config;
    }
}
