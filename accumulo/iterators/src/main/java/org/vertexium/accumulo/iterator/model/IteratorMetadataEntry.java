package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

import java.util.Arrays;
import java.util.Objects;

public class IteratorMetadataEntry {
    public final ByteSequence metadataKey;
    public final ByteSequence metadataVisibility;
    public final byte[] value;

    public IteratorMetadataEntry(ByteSequence metadataKey, ByteSequence metadataVisibility, byte[] value) {
        this.metadataKey = metadataKey;
        this.metadataVisibility = metadataVisibility;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IteratorMetadataEntry that = (IteratorMetadataEntry) o;
        return Objects.equals(metadataKey, that.metadataKey)
            && Objects.equals(metadataVisibility, that.metadataVisibility)
            && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataKey, metadataVisibility);
    }
}
