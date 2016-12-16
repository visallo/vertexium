package org.vertexium.query;

public class RangeResult extends AggregationResult {
    private final Iterable<RangeBucket> buckets;

    public RangeResult(Iterable<RangeBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<RangeBucket> getBuckets() {
        return buckets;
    }

    public RangeBucket getBucketByKey(Object key) {
        String keyStr = key.toString();
        for (RangeBucket rangeBucket : getBuckets()) {
            String bucketKey = rangeBucket.getKey().toString();
            if (bucketKey.equals(keyStr)) {
                return rangeBucket;
            }
        }
        return null;
    }
}
