package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class PropertyMetadataColumnQualifier extends KeyBase {
    public static final int PART_INDEX_PROPERTY_NAME = 0;
    public static final int PART_INDEX_PROPERTY_KEY = 1;
    public static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    public static final int PART_INDEX_METADATA_KEY = 3;
    private final String[] parts;

    public PropertyMetadataColumnQualifier(Text columnQualifier) {
        this.parts = splitOnValueSeparator(columnQualifier.toString(), 4);
    }

    public PropertyMetadataColumnQualifier(String propertyName, String propertyKey, String visibilityString, String metadataKey) {
        parts = new String[]{
                propertyName,
                propertyKey,
                visibilityString,
                metadataKey
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

    public String getMetadataKey() {
        return parts[PART_INDEX_METADATA_KEY];
    }

    public String getPropertyDiscriminator(long propertyTimestamp) {
        return PropertyColumnQualifier.getDiscriminator(getPropertyName(), getPropertyKey(), getPropertyVisibilityString(), propertyTimestamp);
    }
}
