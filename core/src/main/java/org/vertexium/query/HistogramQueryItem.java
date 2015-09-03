package org.vertexium.query;

public class HistogramQueryItem {
    private final String aggregationName;
    private final String fieldName;
    private final String interval;
    private final Long minDocumentCount;

    public HistogramQueryItem(String aggregationName, String fieldName, String interval, Long minDocumentCount) {
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
}
