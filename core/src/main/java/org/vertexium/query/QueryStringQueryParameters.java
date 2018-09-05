package org.vertexium.query;

import org.vertexium.Authorizations;

public class QueryStringQueryParameters extends QueryParameters {
    private final String queryString;

    public QueryStringQueryParameters(String queryString, Authorizations authorizations) {
        super(authorizations);
        this.queryString = queryString;
    }

    public String getQueryString() {
        return queryString;
    }

    public QueryParameters clone() {
        QueryParameters result = new QueryStringQueryParameters(this.getQueryString(), this.getAuthorizations());
        return super.cloneTo(result);
    }

    @Override
    public String toString() {
        return super.toString() +
                ", queryString=" + (queryString == null ? "" : "\"" + queryString + "\"");
    }
}
