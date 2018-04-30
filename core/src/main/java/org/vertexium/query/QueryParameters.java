package org.vertexium.query;

import com.google.common.base.Joiner;
import org.vertexium.Authorizations;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class QueryParameters {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(QueryParameters.class);
    public static final int DEFAULT_SKIP = 0;

    private final Authorizations authorizations;
    private Long limit = null;
    private long skip = DEFAULT_SKIP;
    private final List<QueryBase.HasContainer> hasContainers = new ArrayList<>();
    private final List<QueryBase.SortContainer> sortContainers = new ArrayList<>();
    private final List<String> edgeLabels = new ArrayList<>();
    private List<String> ids;

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

    /**
     * Get the ids of the elements that should be searched in this query.
     *
     * @return null if all elements should be searched. A List of element ids otherwise. Empty list indicates that all elements are filtered out.
     */
    public List<String> getIds() {
        return ids;
    }

    /**
     * When called the first time, all ids are added to the filter.
     * When called two or more times, the provided id's are and'ed with the those provided in the previous lists.
     *
     * @param ids The ids of the elements that should be searched in this query.
     */
    public void addIds(Collection<String> ids) {
        if (this.ids == null) {
            this.ids = new ArrayList<>(ids);
        } else {
            this.ids.retainAll(ids);
            if (this.ids.isEmpty()) {
                LOGGER.warn("No ids remain after addIds. All elements will be filtered out.");
            }
        }
    }

    public abstract QueryParameters clone();

    protected QueryParameters cloneTo(QueryParameters result) {
        result.setSkip(this.getSkip());
        result.setLimit(this.getLimit());
        result.hasContainers.addAll(this.getHasContainers());
        result.sortContainers.addAll(this.getSortContainers());
        result.edgeLabels.addAll(this.getEdgeLabels());
        result.ids = this.ids == null ? null : new ArrayList<>(this.ids);
        return result;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "authorizations=" + authorizations +
                ", limit=" + limit +
                ", skip=" + skip +
                ", hasContainers=" + Joiner.on(", ").join(hasContainers) +
                ", sortContainers=" + Joiner.on(", ").join(sortContainers) +
                ", edgeLabels=" + Joiner.on(", ").join(edgeLabels) +
                ", ids=" + (ids == null  ? null : Joiner.on(", ").join(ids)) +
                '}';
    }
}
