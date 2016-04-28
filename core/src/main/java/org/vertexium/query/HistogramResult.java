package org.vertexium.query;

public class HistogramResult extends AggregationResult {
    private final Iterable<HistogramBucket> buckets;

    public HistogramResult(Iterable<HistogramBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<HistogramBucket> getBuckets() {
        return buckets;
    }

    public HistogramBucket getBucketByKey(Object key) {
        String keyStr = key.toString();
        for (HistogramBucket histogramBucket : getBuckets()) {
            String bucketKey = histogramBucket.getKey().toString();
            if (bucketKey.equals(keyStr)) {
                return histogramBucket;
            }
        }
        return null;
    }
}
