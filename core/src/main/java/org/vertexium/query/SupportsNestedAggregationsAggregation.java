package org.vertexium.query;

public interface SupportsNestedAggregationsAggregation {
    void addNestedAggregation(Aggregation nestedAggregation);

    Iterable<Aggregation> getNestedAggregations();
}
