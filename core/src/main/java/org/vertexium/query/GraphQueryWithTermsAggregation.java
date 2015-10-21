package org.vertexium.query;

@Deprecated
public interface GraphQueryWithTermsAggregation extends GraphQuery {
    @Deprecated
    GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String field);
}
