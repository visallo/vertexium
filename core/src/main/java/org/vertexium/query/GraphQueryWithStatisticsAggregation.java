package org.vertexium.query;

public interface GraphQueryWithStatisticsAggregation extends GraphQuery {
    GraphQueryWithStatisticsAggregation addStatisticsAggregation(String aggregationName, String field);
}
