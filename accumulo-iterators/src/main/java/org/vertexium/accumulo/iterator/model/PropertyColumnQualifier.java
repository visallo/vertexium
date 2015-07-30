package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class PropertyColumnQualifier extends KeyBase {
    public static final int PART_INDEX_PROPERTY_NAME = 0;
    public static final int PART_INDEX_PROPERTY_KEY = 1;
    private final String[] parts;

    public PropertyColumnQualifier(Text columnQualifier) {
        this.parts = splitOnValueSeparator(columnQualifier.toString(), 2);
    }

    public PropertyColumnQualifier(String propertyName, String propertyKey) {
        this.parts = new String[]{
                propertyName,
                propertyKey
        };
    }

    public String getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public String getDiscriminator(String visibilityString, long timestamp) {
        assertNoValueSeparator(getPropertyName());
        assertNoValueSeparator(getPropertyKey());
        assertNoValueSeparator(visibilityString);
        String timestampString = Long.toString(timestamp);
        int length = getPropertyName().length() + 1 + getPropertyKey().length() + 1 + visibilityString.length() + 1 + timestampString.length();
        //noinspection StringBufferReplaceableByString
        return new StringBuilder(length)
                .append(getPropertyName())
                .append(VALUE_SEPARATOR)
                .append(getPropertyKey())
                .append(VALUE_SEPARATOR)
                .append(visibilityString)
                .append(VALUE_SEPARATOR)
                .append(timestampString)
                .toString();
    }
}
