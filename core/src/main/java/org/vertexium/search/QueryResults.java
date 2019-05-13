package org.vertexium.search;

import org.vertexium.query.AggregationResult;

import java.util.stream.Stream;

public interface QueryResults<T> {
    /**
     * This value will be returned from results methods when the answer cannot be determined.
     */
    long UNKNOWN = -1;

    /**
     * This value will be returned from results methods when the score cannot be determined.
     */
    Double UNKNOWN_SCORE = -1.0;

    Stream<T> getHits();

    /**
     * Get the total number of hits for the query that produced these results
     * @return The total number of hits or {@link QueryResults#UNKNOWN} if the answer is cannot be calculated
     */
    long getTotalHits();

    <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType);

    /**
     * Get the query score associated with the provided ID.
     * @param id The id of the element for the score
     * @return The score for the associated search result or  {@link QueryResults#UNKNOWN_SCORE} if the answer is cannot be calculated
     */
    Double getScore(Object id);

    /**
     * Get the amount of time consumed running the query.
     * @return The amount of time in nano-seconds or {@link QueryResults#UNKNOWN} if the answer is cannot be calculated
     */
    long getSearchTimeNanoSeconds();
}
