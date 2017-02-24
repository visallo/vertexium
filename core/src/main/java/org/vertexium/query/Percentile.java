package org.vertexium.query;

public class Percentile {
    private final Double percentile;
    private final Double value;

    public Percentile(Double percentile, Double value) {
        this.percentile = percentile;
        this.value = value;
    }

    public Double getPercentile() {
        return percentile;
    }

    public Double getValue() {
        return value;
    }
}
