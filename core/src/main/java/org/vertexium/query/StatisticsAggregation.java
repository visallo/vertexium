package org.vertexium.query;

public class StatisticsAggregation extends Aggregation {
    private final String aggregationName;
    private final String fieldName;

    public StatisticsAggregation(String aggregationName, String fieldName) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "StatisticsQueryItem{" +
            "aggregationName='" + aggregationName + '\'' +
            ", field='" + fieldName + '\'' +
            '}';
    }
}
