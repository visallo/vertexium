package org.vertexium.query;

@Deprecated
public interface GraphQueryWithStatisticsAggregation extends GraphQuery {
    @Deprecated
    GraphQueryWithStatisticsAggregation addStatisticsAggregation(String aggregationName, String field);
}
