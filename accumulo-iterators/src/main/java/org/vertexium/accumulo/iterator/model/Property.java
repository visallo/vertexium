package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

import java.util.List;
import java.util.Set;

public class Property {
    public final ByteSequence key;
    public final ByteSequence name;
    public final byte[] value;
    public final Set<ByteSequence> hiddenVisibilities;
    public final ByteSequence visibility;
    public final long timestamp;
    public final List<Integer> metadata;

    public Property(
            ByteSequence propertyKey,
            ByteSequence propertyName,
            byte[] propertyValue,
            ByteSequence propertyVisibility,
            long propertyTimestamp,
            Set<ByteSequence> propertyHiddenVisibilities,
            List<Integer> metadata
    ) {
        this.key = propertyKey;
        this.name = propertyName;
        this.value = propertyValue;
        this.visibility = propertyVisibility;
        this.timestamp = propertyTimestamp;
        this.hiddenVisibilities = propertyHiddenVisibilities;
        this.metadata = metadata;
    }
}
