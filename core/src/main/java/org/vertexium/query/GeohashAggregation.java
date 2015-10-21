package org.vertexium.query;

import java.util.ArrayList;
import java.util.List;

public class GeohashAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private final int precision;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();

    public GeohashAggregation(String aggregationName, String fieldName, int precision) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.precision = precision;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public void addNestedAggregation(Aggregation nestedAggregation) {
        this.nestedAggregations.add(nestedAggregation);
    }

    @Override
    public Iterable<Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }
}
