package org.vertexium.query;

public class CardinalityAggregation extends Aggregation {
    private final String aggregationName;
    private final String propertyName;

    public CardinalityAggregation(String aggregationName, String propertyName) {
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
    public String toString() {
        return "CardinalityAggregation{" +
                "aggregationName='" + aggregationName + '\'' +
                ", propertyName='" + propertyName + '\'' +
                '}';
    }
}
