package org.neolumin.vertexium.query;

public class GeohashQueryItem {
    private final String aggregationName;
    private final String fieldName;
    private final int precision;

    public GeohashQueryItem(String aggregationName, String fieldName, int precision) {
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
}
