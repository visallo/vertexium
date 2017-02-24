package org.vertexium.query;

public class PercentilesResult extends AggregationResult {
    private final Iterable<Percentile> percentiles;

    public PercentilesResult(Iterable<Percentile> percentiles) {
        this.percentiles = percentiles;
    }

    public Iterable<Percentile> getPercentiles() {
        return percentiles;
    }
}
