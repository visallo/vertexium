package org.vertexium.query;

@Deprecated
public interface GraphQueryWithGeohashAggregation extends GraphQuery {
    @Deprecated
    GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String field, int precision);
}
