package org.vertexium.sql;

import org.vertexium.Visibility;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

class SqlStreamingPropertyValueRef extends StreamingPropertyValueRef<SqlGraph> {
    private final String elementId;
    private final String key;
    private final String name;
    private final Visibility visibility;
    private final long timestamp;

    public SqlStreamingPropertyValueRef(StreamingPropertyValue propertyValue, String elementId, String key, String name,
                                        Visibility visibility, long timestamp) {
        super(propertyValue);
        this.elementId = elementId;
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.timestamp = timestamp;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(SqlGraph graph, long timestamp) {
        return graph.getStreamingPropertyTable().get(elementId, key, name, visibility, timestamp)
                .store(isStore()).searchIndex(isSearchIndex());
    }
}
