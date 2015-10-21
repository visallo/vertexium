package org.vertexium.query;

import java.util.Map;

public class TermsBucket {
    public final Object key;
    public final long count;
    public final Map<String, AggregationResult> nestedResults;

    public TermsBucket(Object key, long count, Map<String, AggregationResult> nestedResults) {
        this.key = key;
        this.count = count;
        this.nestedResults = nestedResults;
    }

    public Object getKey() {
        return key;
    }

    public long getCount() {
        return count;
    }

    public Map<String, AggregationResult> getNestedResults() {
        return nestedResults;
    }
}
