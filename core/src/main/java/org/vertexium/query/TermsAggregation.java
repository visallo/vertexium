package org.vertexium.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TermsAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String propertyName;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();

    public TermsAggregation(String aggregationName, String propertyName) {
        this.aggregationName = aggregationName;
        this.propertyName = propertyName;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getPropertyName() {
        return propertyName;
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
