package org.vertexium.query;

import org.vertexium.User;

public class SimilarToTextQueryParameters extends SimilarToQueryParameters {
    private final String text;

    public SimilarToTextQueryParameters(String[] fields, String text, User user) {
        super(fields, user);
        this.text = text;
    }

    public String getText() {
        return text;
    }

    @Override
    public QueryParameters clone() {
        SimilarToTextQueryParameters results = new SimilarToTextQueryParameters(getFields(), getText(), getUser());
        super.cloneTo(results);
        return results;
    }
}
