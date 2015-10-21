package org.vertexium.query;

public class GeohashResult extends AggregationResult {
    private final Iterable<GeohashBucket> buckets;

    public GeohashResult(Iterable<GeohashBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<GeohashBucket> getBuckets() {
        return buckets;
    }

    public long getMaxCount() {
        long max = Long.MIN_VALUE;
        for (GeohashBucket b : getBuckets()) {
            max = Math.max(max, b.getCount());
        }
        return max;
    }
}
