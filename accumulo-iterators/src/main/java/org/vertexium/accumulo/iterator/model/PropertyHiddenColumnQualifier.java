package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class PropertyHiddenColumnQualifier extends KeyBase {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private final String[] parts;

    public PropertyHiddenColumnQualifier(Text columnQualifier) {
        parts = splitOnValueSeparator(columnQualifier.toString(), 3);
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
