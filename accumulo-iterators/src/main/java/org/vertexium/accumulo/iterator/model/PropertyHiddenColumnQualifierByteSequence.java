package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

public class PropertyHiddenColumnQualifierByteSequence extends KeyBaseByteSequence {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private final ByteSequence[] parts;

    public PropertyHiddenColumnQualifierByteSequence(ByteSequence columnQualifier) {
        parts = splitOnValueSeparator(columnQualifier, 3);
    }

    public ByteSequence getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public ByteSequence getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public ByteSequence getPropertyVisibilityString() {
        return parts[PART_INDEX_PROPERTY_VISIBILITY];
    }
}
