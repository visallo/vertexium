package org.vertexium;

import java.io.Serializable;
import java.util.Set;

public class HistoricalPropertyValue implements Serializable, Comparable<HistoricalPropertyValue> {
    static final long serialVersionUID = 42L;
    private final long timestamp;
    private final Object value;
    private final Metadata metadata;
    private Set<Visibility> hiddenVisibilities;

    public HistoricalPropertyValue(long timestamp, Object value, Metadata metadata, Set<Visibility> hiddenVisibilities) {
        this.timestamp = timestamp;
        this.value = value;
        this.metadata = metadata;
        this.hiddenVisibilities = hiddenVisibilities;
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
    public int compareTo(HistoricalPropertyValue o) {
        return -Long.compare(getTimestamp(), o.getTimestamp());
    }

    @Override
    public String toString() {
        return "HistoricalPropertyValue{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
