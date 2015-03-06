package org.neolumin.vertexium.query;

import org.neolumin.vertexium.Authorizations;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryParameters {
    private final Authorizations authorizations;
    private long limit = 100;
    private long skip = 0;
    private final List<QueryBase.HasContainer> hasContainers = new ArrayList<>();

    public QueryParameters(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public void addHasContainer(QueryBase.HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public long getSkip() {
        return skip;
    }

    public void setSkip(long skip) {
        this.skip = skip;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public List<QueryBase.HasContainer> getHasContainers() {
        return hasContainers;
    }

    public abstract QueryParameters clone();

    protected QueryParameters cloneTo(QueryParameters result) {
        result.setSkip(this.getSkip());
        result.setLimit(this.getLimit());
        result.hasContainers.addAll(this.getHasContainers());
        return result;
    }
}
