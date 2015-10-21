package org.vertexium.query;

import org.vertexium.VertexiumException;

import java.util.ArrayList;

public abstract class AggregationResult {
    public static <TResult extends AggregationResult> TResult createEmptyResult(Class<? extends TResult> resultType) {
        if (resultType.equals(TermsResult.class)) {
            return resultType.cast(new TermsResult(new ArrayList<TermsBucket>()));
        }
        if (resultType.equals(StatisticsResult.class)) {
            return resultType.cast(new StatisticsResult(0, 0.0, 0.0, 0.0, 0.0));
        }
        if (resultType.equals(HistogramResult.class)) {
            return resultType.cast(new HistogramResult(new ArrayList<HistogramBucket>()));
        }
        if (resultType.equals(GeohashResult.class)) {
            return resultType.cast(new GeohashResult(new ArrayList<GeohashBucket>()));
        }
        throw new VertexiumException("Unhandled type to create empty results for: " + resultType.getName());
    }
}
