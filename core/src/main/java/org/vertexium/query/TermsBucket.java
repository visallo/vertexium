package org.vertexium.query;

public class TermsBucket {
    public final Object key;
    public final long count;

    public TermsBucket(Object key, long count) {
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
