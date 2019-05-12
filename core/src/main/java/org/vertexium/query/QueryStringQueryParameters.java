package org.vertexium.query;

import org.vertexium.User;

public class QueryStringQueryParameters extends QueryParameters {
    private final String queryString;

    public QueryStringQueryParameters(String queryString, User user) {
        super(user);
        this.queryString = queryString;
    }

    public String getQueryString() {
        return queryString;
    }

    public QueryParameters clone() {
        QueryParameters result = new QueryStringQueryParameters(this.getQueryString(), this.getUser());
        return super.cloneTo(result);
    }

    @Override
    public String toString() {
        return super.toString() +
            ", queryString=" + (queryString == null ? "" : "\"" + queryString + "\"");
    }
}
