package org.neolumin.vertexium;

import java.io.Serializable;

public class HistoricalPropertyValue implements Serializable, Comparable<HistoricalPropertyValue> {
    static final long serialVersionUID = 42L;
    private final long timestamp;
    private final Object value;
    private final Metadata metadata;

    public HistoricalPropertyValue(long timestamp, Object value, Metadata metadata) {
        this.timestamp = timestamp;
        this.value = value;
        this.metadata = metadata;
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
