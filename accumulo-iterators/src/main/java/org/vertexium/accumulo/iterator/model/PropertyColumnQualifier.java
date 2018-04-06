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
        return getDiscriminator(getPropertyName(), getPropertyKey(), visibilityString, timestamp);
    }

    public static String getDiscriminator(String propertyName, String propertyKey, String visibilityString, long timestamp) {
        assertNoValueSeparator(propertyName);
        assertNoValueSeparator(propertyKey);
        assertNoValueSeparator(visibilityString);
        String timestampString = Long.toHexString(timestamp);
        int length = propertyName.length() + 1 + propertyKey.length() + 1 + visibilityString.length() + 1 + timestampString.length();
        //noinspection StringBufferReplaceableByString
        return new StringBuilder(length)
                .append(propertyName)
                .append(VALUE_SEPARATOR)
                .append(propertyKey)
                .append(VALUE_SEPARATOR)
                .append(visibilityString)
                .append(VALUE_SEPARATOR)
                .append(timestampString)
                .toString();
    }
}
