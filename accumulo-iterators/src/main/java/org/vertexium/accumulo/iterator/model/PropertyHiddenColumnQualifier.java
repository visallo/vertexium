package org.vertexium.accumulo.iterator.model;

public class PropertyHiddenColumnQualifier extends KeyBase {
    public static final int PART_INDEX_PROPERTY_NAME = 0;
    public static final int PART_INDEX_PROPERTY_KEY = 1;
    public static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private final String[] parts;

    public PropertyHiddenColumnQualifier(String columnQualifier) {
        parts = splitOnValueSeparator(columnQualifier, 3);
    }

    public PropertyHiddenColumnQualifier(String propertyName, String propertyKey, String visibilityString) {
        parts = new String[]{
            propertyName,
            propertyKey,
            visibilityString
        };
    }

    public String getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public String getPropertyVisibilityString() {
        return parts[PART_INDEX_PROPERTY_VISIBILITY];
    }
}
