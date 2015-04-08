package org.vertexium.query;

public class HistogramBucket {
    public final Object key;
    public final long count;

    public HistogramBucket(Object key, long count) {
        this.key = key;
        this.count = count;
    }

    public Object getKey() {
        return key;
    }

    public long getCount() {
        return count;
    }
}
