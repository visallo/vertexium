package org.vertexium.query;

public interface SimilarToGraphQuery extends GraphQuery {
    /**
     * The minimum number of times a term must appear in the source data to be considered for a match.
     */
    SimilarToGraphQuery minTermFrequency(int minTermFrequency);

    /**
     * The maximum number of terms to be searched for.
     */
    SimilarToGraphQuery maxQueryTerms(int maxQueryTerms);

    /**
     * The minimum number of documents a term must be in to be considered for a similarity match.
     */
    SimilarToGraphQuery minDocFrequency(int minDocFrequency);

    /**
     * The maximum number of documents a term can be in to be considered for a similarity match.
     */
    SimilarToGraphQuery maxDocFrequency(int maxDocFrequency);

    /**
     * The percentage of terms that must match to be considered similar.
     */
    SimilarToGraphQuery percentTermsToMatch(float percentTermsToMatch);

    /**
     * The amount of boost to apply to the similarity query.
     */
    SimilarToGraphQuery boost(float boost);
}
