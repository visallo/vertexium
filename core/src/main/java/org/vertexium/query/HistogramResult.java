package org.vertexium.query;

public class HistogramResult extends AggregationResult {
    private final Iterable<HistogramBucket> buckets;

    public HistogramResult(Iterable<HistogramBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<HistogramBucket> getBuckets() {
        return buckets;
    }
}
