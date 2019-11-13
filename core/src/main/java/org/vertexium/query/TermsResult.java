package org.vertexium.query;

public class TermsResult extends AggregationResult {
    private final Iterable<TermsBucket> buckets;
    private final long sumOfOtherDocCounts;
    private final long docCountErrorUpperBound;

    public TermsResult(Iterable<TermsBucket> buckets) {
        this.buckets = buckets;
        this.sumOfOtherDocCounts = 0;
        this.docCountErrorUpperBound = 0;
    }

    public TermsResult(Iterable<TermsBucket> buckets, long sumOfOtherDocCounts, long docCountErrorUpperBound) {
        this.buckets = buckets;
        this.sumOfOtherDocCounts = sumOfOtherDocCounts;
        this.docCountErrorUpperBound = docCountErrorUpperBound;
    }

    public Iterable<TermsBucket> getBuckets() {
        return buckets;
    }

    public long getSumOfOtherDocCounts() {
        return sumOfOtherDocCounts;
    }

    public long getDocCountErrorUpperBound() {
        return docCountErrorUpperBound;
    }
}
