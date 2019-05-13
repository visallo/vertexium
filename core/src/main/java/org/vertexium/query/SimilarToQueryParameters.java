package org.vertexium.query;

import org.vertexium.User;

public abstract class SimilarToQueryParameters extends QueryParameters {
    private final String[] fields;
    private Integer minTermFrequency;
    private Integer maxQueryTerms;
    private Integer minDocFrequency;
    private Integer maxDocFrequency;
    private Float boost;

    protected SimilarToQueryParameters(String[] fields, User user) {
        super(user);
        this.fields = fields;
    }

    public String[] getFields() {
        return fields;
    }

    public Integer getMinTermFrequency() {
        return minTermFrequency;
    }

    public void setMinTermFrequency(Integer minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
    }

    public Integer getMaxQueryTerms() {
        return maxQueryTerms;
    }

    public void setMaxQueryTerms(Integer maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }

    public Integer getMinDocFrequency() {
        return minDocFrequency;
    }

    public void setMinDocFrequency(Integer minDocFrequency) {
        this.minDocFrequency = minDocFrequency;
    }

    public Integer getMaxDocFrequency() {
        return maxDocFrequency;
    }

    public void setMaxDocFrequency(Integer maxDocFrequency) {
        this.maxDocFrequency = maxDocFrequency;
    }

    public Float getBoost() {
        return boost;
    }

    public void setBoost(Float boost) {
        this.boost = boost;
    }

    @Override
    protected QueryParameters cloneTo(QueryParameters result) {
        ((SimilarToQueryParameters) result).minTermFrequency = getMinTermFrequency();
        ((SimilarToQueryParameters) result).maxQueryTerms = getMaxQueryTerms();
        ((SimilarToQueryParameters) result).minDocFrequency = getMinDocFrequency();
        ((SimilarToQueryParameters) result).maxDocFrequency = getMaxDocFrequency();
        ((SimilarToQueryParameters) result).boost = getBoost();
        return super.cloneTo(result);
    }
}
