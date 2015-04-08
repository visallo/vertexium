package org.vertexium.query;

public class TermsQueryItem {
    private final String aggregationName;
    private final String fieldName;

    public TermsQueryItem(String aggregationName, String fieldName) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }
}
