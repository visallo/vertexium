package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class SoftDeletedProperty {
    private final String propertyKey;
    private final String propertyName;
    private final long timestamp;
    private final Text visibility;

    public SoftDeletedProperty(String propertyKey, String propertyName, long timestamp, Text visibility) {
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

        if (propertyKey != null ? !propertyKey.equals(that.propertyKey) : that.propertyKey != null) {
            return false;
        }
        if (propertyName != null ? !propertyName.equals(that.propertyName) : that.propertyName != null) {
            return false;
        }
        if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) {
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

    public boolean matches(String propertyKey, String propertyName, Text visibility) {
        return propertyKey.equals(this.propertyKey)
                && propertyName.equals(this.propertyName)
                && visibility.equals(this.visibility);
    }
}

