package org.vertexium.query;

@Deprecated
public interface GraphQueryWithHistogramAggregation extends GraphQuery {
    @Deprecated
    GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String field, String interval);

    @Deprecated
    GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String field, String interval, Long minDocumentCount);
}
