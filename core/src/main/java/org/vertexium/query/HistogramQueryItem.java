package org.vertexium.query;

public class HistogramQueryItem {
    private final String aggregationName;
    private final String fieldName;
    private final String interval;

    public HistogramQueryItem(String aggregationName, String fieldName, String interval) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.interval = interval;
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
}
