package org.vertexium.query;

import org.vertexium.Authorizations;

public class SimilarToTextQueryParameters extends SimilarToQueryParameters {
    private final String text;

    public SimilarToTextQueryParameters(String[] fields, String text, Authorizations authorizations) {
        super(fields, authorizations);
        this.text = text;
    }

    public String getText() {
        return text;
    }

    @Override
    public QueryParameters clone() {
        SimilarToTextQueryParameters results = new SimilarToTextQueryParameters(getFields(), getText(), getAuthorizations());
        super.cloneTo(results);
        return results;
    }
}
