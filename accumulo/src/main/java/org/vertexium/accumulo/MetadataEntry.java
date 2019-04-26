package org.vertexium.accumulo;

import org.vertexium.VertexiumSerializer;
import org.vertexium.Visibility;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.Arrays;
import java.util.Objects;

public class MetadataEntry {
    private final String key;
    private final String visibility;
    private final byte[] value;
    private transient String realizedKey;
    private transient Visibility realizedVisibility;
    private transient Object realizedValue;

    public MetadataEntry(String key, String visibility, byte[] value) {
        this.key = key;
        this.visibility = visibility;
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
        MetadataEntry that = (MetadataEntry) o;
        return Objects.equals(key, that.key)
            && Objects.equals(visibility, that.visibility)
            && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, visibility);
    }

    public String getMetadataKey(NameSubstitutionStrategy nameSubstitutionStrategy) {
        if (realizedKey != null) {
            return realizedKey;
        }
        realizedKey = nameSubstitutionStrategy.inflate(key);
        return realizedKey;
    }

    public Visibility getVisibility() {
        if (realizedVisibility != null) {
            return realizedVisibility;
        }
        realizedVisibility = new Visibility(visibility);
        return realizedVisibility;
    }

    public Object getValue(VertexiumSerializer vertexiumSerializer) {
        if (realizedValue != null) {
            return realizedValue;
        }
        realizedValue = vertexiumSerializer.bytesToObject(value);
        return realizedValue;
    }
}
