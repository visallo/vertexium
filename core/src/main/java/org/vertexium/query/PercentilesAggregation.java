package org.vertexium.query;

import org.vertexium.Visibility;

public class PercentilesAggregation extends Aggregation {
    private final String aggregationName;
    private final String fieldName;
    private final Visibility visibility;

    private double[] percents;

    public PercentilesAggregation(String aggregationName, String fieldName, Visibility visibility) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.visibility = visibility;
    }

    @Override
    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public double[] getPercents() {
        return percents;
    }

    public void setPercents(double... percents) {
        this.percents = percents;
    }
}
