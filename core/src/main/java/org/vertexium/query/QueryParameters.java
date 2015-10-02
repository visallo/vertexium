package org.vertexium.query;

import org.vertexium.Authorizations;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryParameters {
    public static final int DEFAULT_SKIP = 0;

    private final Authorizations authorizations;
    private Long limit = null;
    private long skip = DEFAULT_SKIP;
    private final List<QueryBase.HasContainer> hasContainers = new ArrayList<>();
    private final List<QueryBase.SortContainer> sortContainers = new ArrayList<>();
    private final List<String> edgeLabels = new ArrayList<>();

    public QueryParameters(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public void addHasContainer(QueryBase.HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        if (limit == null) {
            this.limit = null;
        } else {
            this.limit = (long) limit;
        }
    }

    public void setLimit(Long limit) {
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

    public List<QueryBase.SortContainer> getSortContainers() {
        return sortContainers;
    }

    public void addSortContainer(QueryBase.SortContainer sortContainer) {
        sortContainers.add(sortContainer);
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
    }

    public void addEdgeLabel(String edgeLabel) {
        this.edgeLabels.add(edgeLabel);
    }

    public abstract QueryParameters clone();

    protected QueryParameters cloneTo(QueryParameters result) {
        result.setSkip(this.getSkip());
        result.setLimit(this.getLimit());
        result.hasContainers.addAll(this.getHasContainers());
        result.sortContainers.addAll(this.getSortContainers());
        return result;
    }
}
