package org.vertexium.elasticsearch.plugin;

import java.util.Map;
import java.util.Set;

public class FieldNameToVisibilityMap {
    private final Map<String, String> fieldNameToVisibilityMapping;

    public FieldNameToVisibilityMap(Map<String, String> fieldNameToVisibilityMapping) {
        this.fieldNameToVisibilityMapping = fieldNameToVisibilityMapping;
    }

    public Set<String> getFieldNames() {
        return fieldNameToVisibilityMapping.keySet();
    }

    public String getFieldVisibility(String fieldName) {
        return fieldNameToVisibilityMapping.get(fieldName);
    }

    @SuppressWarnings("unchecked")
    public static FieldNameToVisibilityMap createFromVertexiumMetadata(Object vertexiumMeta) {
        return new FieldNameToVisibilityMap((Map<String, String>) vertexiumMeta);
    }
}
