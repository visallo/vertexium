package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.GraphConfiguration;
import org.vertexium.PropertyDefinition;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class DefaultIndexSelectionStrategy implements IndexSelectionStrategy {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DefaultIndexSelectionStrategy.class);
    public static final String CONFIG_INDEX_NAME = "indexName";
    public static final String DEFAULT_INDEX_NAME = "vertexium";
    private String[] indicesToQuery;

    public DefaultIndexSelectionStrategy(GraphConfiguration config) {
        indicesToQuery = new String[]{
                getDefaultIndexName(config)
        };
    }

    private static String getDefaultIndexName(GraphConfiguration config) {
        String defaultIndexName = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.info("Default index name: %s", defaultIndexName);
        return defaultIndexName;
    }

    @Override
    public String[] getIndicesToQuery() {
        return indicesToQuery;
    }

    @Override
    public String getIndexName(Element element) {
        return indicesToQuery[0];
    }

    @Override
    public String[] getIndexNames(PropertyDefinition propertyDefinition) {
        return indicesToQuery;
    }
}
