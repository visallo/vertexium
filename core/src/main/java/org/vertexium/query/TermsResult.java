package org.vertexium.query;

public class TermsResult extends AggregationResult {
    private final Iterable<TermsBucket> buckets;

    public TermsResult(Iterable<TermsBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<TermsBucket> getBuckets() {
        return buckets;
    }
}
