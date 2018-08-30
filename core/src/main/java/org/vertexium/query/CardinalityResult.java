package org.vertexium.query;

public class CardinalityResult extends AggregationResult {
    private final long count;

    public CardinalityResult(long count) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }
}
