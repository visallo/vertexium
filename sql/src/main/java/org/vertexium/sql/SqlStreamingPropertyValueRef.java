package org.vertexium.sql;

import org.vertexium.Visibility;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

class SqlStreamingPropertyValueRef extends StreamingPropertyValueRef<SqlGraph> {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlStreamingPropertyValueRef.class);
    private final String elementId;
    private final String key;
    private final String name;
    private final Visibility visibility;
    private final long timestamp;

    public SqlStreamingPropertyValueRef(
            StreamingPropertyValue propertyValue,
            String elementId,
            String key,
            String name,
            Visibility visibility,
            long timestamp
    ) {
        super(propertyValue);
        this.elementId = elementId;
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.timestamp = timestamp;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(SqlGraph graph, long timestamp) {
        StreamingPropertyValue spv = graph.getStreamingPropertyTable()
                .get(elementId, key, name, visibility, timestamp);
        if (spv == null) {
            LOGGER.warn("Could not find SQL SPV with the property timestamp %d, using ref timestamp instead", timestamp, this.timestamp);
            spv = graph.getStreamingPropertyTable()
                    .get(elementId, key, name, visibility, this.timestamp);
        }
        return spv
                .store(isStore())
                .searchIndex(isSearchIndex());
    }
}
