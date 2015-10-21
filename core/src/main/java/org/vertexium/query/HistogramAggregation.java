package org.vertexium.query;

import java.util.ArrayList;
import java.util.List;

public class HistogramAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private final String interval;
    private final Long minDocumentCount;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();

    public HistogramAggregation(String aggregationName, String fieldName, String interval, Long minDocumentCount) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.interval = interval;
        this.minDocumentCount = minDocumentCount;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getInterval() {
        return interval;
    }

    public Long getMinDocumentCount() {
        return minDocumentCount;
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
