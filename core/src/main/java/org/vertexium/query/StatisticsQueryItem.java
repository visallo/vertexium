package org.vertexium.query;

public class StatisticsQueryItem {
    private final String aggregationName;
    private final String fieldName;

    public StatisticsQueryItem(String aggregationName, String fieldName) {
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
