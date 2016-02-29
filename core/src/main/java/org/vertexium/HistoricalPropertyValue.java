package org.vertexium;

import java.io.Serializable;
import java.util.Set;

public class HistoricalPropertyValue implements Serializable, Comparable<HistoricalPropertyValue> {
    static final long serialVersionUID = 42L;
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;
    private final long timestamp;
    private final Object value;
    private final Metadata metadata;
    private Set<Visibility> hiddenVisibilities;

    public HistoricalPropertyValue(
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            long timestamp,
            Object value,
            Metadata metadata,
            Set<Visibility> hiddenVisibilities
    ) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
        this.timestamp = timestamp;
        this.value = value;
        this.metadata = metadata;
        this.hiddenVisibilities = hiddenVisibilities;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Set<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HistoricalPropertyValue that = (HistoricalPropertyValue) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (!propertyKey.equals(that.propertyKey)) {
            return false;
        }
        if (!propertyName.equals(that.propertyName)) {
            return false;
        }
        if (!propertyVisibility.equals(that.propertyVisibility)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = propertyKey.hashCode();
        result = 31 * result + propertyName.hashCode();
        result = 31 * result + propertyVisibility.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public int compareTo(HistoricalPropertyValue o) {
        int result = -Long.compare(getTimestamp(), o.getTimestamp());
        if (result != 0) {
            return result;
        }

        result = getPropertyName().compareTo(o.getPropertyName());
        if (result != 0) {
            return result;
        }

        result = getPropertyKey().compareTo(o.getPropertyKey());
        if (result != 0) {
            return result;
        }

        result = getPropertyVisibility().compareTo(o.getPropertyVisibility());
        if (result != 0) {
            return result;
        }

        return 0;
    }

    @Override
    public String toString() {
        return "HistoricalPropertyValue{" +
                "propertyKey='" + propertyKey + '\'' +
                ", propertyName='" + propertyName + '\'' +
                ", propertyVisibility=" + propertyVisibility +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
