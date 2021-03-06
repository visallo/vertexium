package org.vertexium.query;

public class TermsResult extends AggregationResult {
    public static long NOT_COMPUTED = -1;
    private final Iterable<TermsBucket> buckets;
    private final long sumOfOtherDocCounts;
    private final long docCountErrorUpperBound;
    private final long hasNotCount;

    public TermsResult(Iterable<TermsBucket> buckets) {
        this.buckets = buckets;
        this.sumOfOtherDocCounts = 0;
        this.docCountErrorUpperBound = 0;
        this.hasNotCount = -1;
    }

    public TermsResult(Iterable<TermsBucket> buckets, long sumOfOtherDocCounts, long docCountErrorUpperBound, long hasNotCount) {
        this.buckets = buckets;
        this.sumOfOtherDocCounts = sumOfOtherDocCounts;
        this.docCountErrorUpperBound = docCountErrorUpperBound;
        this.hasNotCount = hasNotCount;
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

    /**
     * @return If the parameter includeHasNotCount was set to true on the TermsAggregation, this will
     *         return the number of elements that do not have a value for the property being aggregated.
     *         If that parameter was false, this will return NOT_COMPUTED (-1).
     */
    public long getHasNotCount() {
        return hasNotCount;
    }
}
