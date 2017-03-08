package org.vertexium;

import java.io.Serializable;
import java.util.Set;

/**
 * HistoricalPropertyValues are intended to be snapshots of property values in time.
 * That is, all fields must reflect the state of the property at a given time. This
 * is important as differences between consecutive historicalPropertyValues are seen
 * as property changes.
 */
public class HistoricalPropertyValue implements Serializable, Comparable<HistoricalPropertyValue> {
    static final long serialVersionUID = 42L;
    private final String propertyKey;               // required
    private final String propertyName;              // required
    private final long timestamp;                   // required
    private final Visibility propertyVisibility;
    private final Object value;
    private final Metadata metadata;
    private final Boolean isDeleted;
    private Set<Visibility> hiddenVisibilities;

    public HistoricalPropertyValue(HistoricalPropertyValueBuilder builder) {
        this.propertyKey = builder.propertyKey;
        this.propertyName = builder.propertyName;
        this.propertyVisibility = builder.propertyVisibility;
        this.timestamp = builder.timestamp;
        this.value = builder.value;
        this.metadata = builder.metadata;
        this.hiddenVisibilities = builder.hiddenVisibilities;
        this.isDeleted = builder.isDeleted;
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

    public boolean isDeleted() {
        return (isDeleted != null && isDeleted);
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
                ", isDeleted=" + isDeleted +
                '}';
    }

    public static class HistoricalPropertyValueBuilder {
        private String propertyKey;
        private String propertyName;
        private long timestamp;
        private Visibility propertyVisibility;
        private Object value;
        private Metadata metadata;
        private Boolean isDeleted;
        private Set<Visibility> hiddenVisibilities;

        public HistoricalPropertyValueBuilder(String propertyKey, String propertyName, long timestamp) {
            this.propertyKey = propertyKey;
            this.propertyName = propertyName;
            this.timestamp = timestamp;
        }

        public HistoricalPropertyValueBuilder value(Object value) {
            this.value = value;
            return this;
        }
        public HistoricalPropertyValueBuilder metadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }
        public HistoricalPropertyValueBuilder isDeleted(Boolean isDeleted) {
            this.isDeleted = isDeleted;
            return this;
        }
        public HistoricalPropertyValueBuilder hiddenVisibilities(Set<Visibility> hiddenVisibilities) {
            this.hiddenVisibilities = hiddenVisibilities;
            return this;
        }

        public HistoricalPropertyValueBuilder propertyVisibility(Visibility propertyVisibility) {
            this.propertyVisibility = propertyVisibility;
            return this;
        }

        public HistoricalPropertyValueBuilder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public HistoricalPropertyValue build() {
            return new HistoricalPropertyValue(this);
        }
    }
}
