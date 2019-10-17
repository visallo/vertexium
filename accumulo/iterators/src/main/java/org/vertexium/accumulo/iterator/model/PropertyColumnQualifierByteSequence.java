package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

public class PropertyColumnQualifierByteSequence extends KeyBaseByteSequence {
    public static final int PART_INDEX_PROPERTY_NAME = 0;
    public static final int PART_INDEX_PROPERTY_KEY = 1;
    private final ByteSequence[] parts;

    public PropertyColumnQualifierByteSequence(ByteSequence columnQualifier) {
        this.parts = splitOnValueSeparator(columnQualifier, 2);
    }

    public ByteSequence getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public ByteSequence getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public ByteSequence getDiscriminator(ByteSequence columnVisibility, long timestamp) {
        return getDiscriminator(getPropertyName(), getPropertyKey(), columnVisibility, timestamp);
    }
}
