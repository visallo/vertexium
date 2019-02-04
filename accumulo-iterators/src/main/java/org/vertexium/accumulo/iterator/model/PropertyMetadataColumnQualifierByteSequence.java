package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

public class PropertyMetadataColumnQualifierByteSequence extends KeyBaseByteSequence {
    public static final int PART_INDEX_PROPERTY_NAME = 0;
    public static final int PART_INDEX_PROPERTY_KEY = 1;
    public static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    public static final int PART_INDEX_METADATA_KEY = 3;
    private final ByteSequence[] parts;

    public PropertyMetadataColumnQualifierByteSequence(ByteSequence columnQualifier) {
        this.parts = splitOnValueSeparator(columnQualifier, 4);
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

    public ByteSequence getMetadataKey() {
        return parts[PART_INDEX_METADATA_KEY];
    }

    public ByteSequence getPropertyDiscriminator(long propertyTimestamp) {
        return getDiscriminator(getPropertyName(), getPropertyKey(), getPropertyVisibilityString(), propertyTimestamp);
    }
}
