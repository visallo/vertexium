package org.vertexium.query;

import java.util.Map;

public class RangeBucket {
    private final Object key;
    private final long count;
    private final Map<String, AggregationResult> nestedResults;

    public RangeBucket(Object key, long count, Map<String, AggregationResult> nestedResults) {
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
