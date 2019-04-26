package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

import java.util.Objects;

public class SoftDeletedProperty {
    private final ByteSequence propertyKey;
    private final ByteSequence propertyName;
    private final long timestamp;
    private final ByteSequence visibility;

    public SoftDeletedProperty(ByteSequence propertyKey, ByteSequence propertyName, long timestamp, ByteSequence visibility) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.timestamp = timestamp;
        this.visibility = visibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SoftDeletedProperty that = (SoftDeletedProperty) o;

        if (!Objects.equals(propertyKey, that.propertyKey)) {
            return false;
        }
        if (!Objects.equals(propertyName, that.propertyName)) {
            return false;
        }
        if (!Objects.equals(visibility, that.visibility)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = propertyKey != null ? propertyKey.hashCode() : 0;
        result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        return result;
    }

    public boolean matches(ByteSequence propertyKey, ByteSequence propertyName, ByteSequence visibility) {
        return propertyKey.equals(this.propertyKey)
            && propertyName.equals(this.propertyName)
            && visibility.equals(this.visibility);
    }
}

