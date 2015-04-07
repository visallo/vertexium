package org.neolumin.vertexium;

public class HistoricalPropertyValue implements Comparable<HistoricalPropertyValue> {
    private final long timestamp;
    private final Object value;

    public HistoricalPropertyValue(long timestamp, Object value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
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
