package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.QueryBase;
import org.vertexium.query.*;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.search.QueryResults;
import org.vertexium.util.CloseableIterator;
import org.vertexium.util.CloseableUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.Preconditions.checkNotNull;

public class DefaultGraphQueryResults<T> implements QueryResults<T> {
    private final QueryParameters parameters;
    private final Stream<T> objects;
    private final boolean evaluateQueryString;
    private final boolean evaluateHasContainers;


    public DefaultGraphQueryResults(
        QueryParameters parameters,
        Stream<T> objects,
        boolean evaluateQueryString,
        boolean evaluateHasContainers,
        boolean evaluateSortContainers
    ) {
        checkNotNull(objects, "objects cannot be null");
        this.parameters = parameters;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        if (evaluateSortContainers && this.parameters.getSortContainers().size() > 0) {
            this.objects = sortUsingSortContainers(objects, parameters.getSortContainers());
        } else if (evaluateHasContainers && this.parameters.getScoringStrategy() != null) {
            this.objects = sortUsingScoringStrategy(objects, parameters.getScoringStrategy());
        } else {
            this.objects = objects;
        }
    }

    private Stream<T> sortUsingScoringStrategy(Stream<T> objects, ScoringStrategy scoringStrategy) {
        return objects.sorted(new ScoringStrategyComparator<>(scoringStrategy));
    }

    private Stream<T> sortUsingSortContainers(Stream<T> objects, List<QueryBase.SortContainer> sortContainers) {
        return objects.sorted(new SortContainersComparator<>(sortContainers));
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(false);
    }

    protected Iterator<T> iterator(final boolean iterateAll) {
        final Iterator<T> it = objects.iterator();

        return new CloseableIterator<T>() {
            public T next;
            public T current;
            public long count;

            @Override
            public boolean hasNext() {
                loadNext();
                if (next == null) {
                    close();
                }
                return next != null;
            }

            @Override
            public T next() {
                loadNext();
                if (next == null) {
                    throw new NoSuchElementException();
                }
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                CloseableUtils.closeQuietly(it);
                DefaultGraphQueryResults.this.close();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                if (!iterateAll && parameters.getLimit() != null && (this.count >= parameters.getSkip() + parameters.getLimit())) {
                    return;
                }

                while (it.hasNext()) {
                    T elem = it.next();
                    VertexiumObject vertexiumElem = elem instanceof VertexiumObject ? (VertexiumObject) elem : null;

                    boolean match = true;
                    if (evaluateHasContainers && vertexiumElem != null) {
                        for (QueryBase.HasContainer has : parameters.getHasContainers()) {
                            if (!has.isMatch(vertexiumElem)) {
                                match = false;
                                break;
                            }
                        }
                        if (vertexiumElem instanceof Edge && parameters.getEdgeLabels().size() > 0) {
                            Edge edge = (Edge) vertexiumElem;
                            if (!parameters.getEdgeLabels().contains(edge.getLabel())) {
                                match = false;
                            }
                        }
                        if (parameters.getIds() != null) {
                            if (vertexiumElem instanceof Element) {
                                if (!parameters.getIds().contains(((Element) vertexiumElem).getId())) {
                                    match = false;
                                }
                            } else if (vertexiumElem instanceof ExtendedDataRow) {
                                if (!parameters.getIds().contains(((ExtendedDataRow) vertexiumElem).getId().getElementId())) {
                                    match = false;
                                }
                            } else {
                                throw new VertexiumException("Unhandled element type: " + vertexiumElem.getClass().getName());
                            }
                        }

                        if (parameters.getMinScore() != null) {
                            if (parameters.getScoringStrategy() == null) {
                                match = false;
                            } else {
                                Double elementScore = parameters.getScoringStrategy().getScore(vertexiumElem);
                                if (elementScore == null) {
                                    match = false;
                                } else {
                                    match = elementScore >= parameters.getMinScore();
                                }
                            }
                        }
                    }
                    if (!match) {
                        continue;
                    }
                    if (evaluateQueryString
                        && vertexiumElem != null
                        && parameters instanceof QueryStringQueryParameters
                        && ((QueryStringQueryParameters) parameters).getQueryString() != null
                        && !evaluateQueryString(vertexiumElem, ((QueryStringQueryParameters) parameters).getQueryString())
                    ) {
                        continue;
                    }

                    this.count++;
                    if (!iterateAll && (this.count <= parameters.getSkip())) {
                        continue;
                    }

                    this.next = elem;
                    break;
                }
            }
        };
    }

    protected boolean evaluateQueryString(VertexiumObject vertexiumObject, String queryString) {
        if (vertexiumObject instanceof Element) {
            return evaluateQueryString((Element) vertexiumObject, queryString);
        } else if (vertexiumObject instanceof ExtendedDataRow) {
            return evaluateQueryString((ExtendedDataRow) vertexiumObject, queryString);
        } else {
            throw new VertexiumException("Unhandled VertexiumObject type: " + vertexiumObject.getClass().getName());
        }
    }

    private boolean evaluateQueryString(Element element, String queryString) {
        for (Property property : element.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryString(ExtendedDataRow extendedDataRow, String queryString) {
        for (Property property : extendedDataRow.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryStringOnValue(Object value, String queryString) {
        if (value == null) {
            return false;
        }
        if (queryString.equals("*")) {
            return true;
        }
        if (value instanceof StreamingPropertyValue) {
            value = ((StreamingPropertyValue) value).readToString();
        }
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }

    @Override
    public Stream<T> getHits() {
        return objects;
    }

    @Override
    public long getTotalHits() {
        // a limit could be set on a query which could prevent all items being returned
        return count(this.iterator(true));
    }

    @Override
    public void close() {
        CloseableUtils.closeQuietly(objects);
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        throw new VertexiumException("Could not find aggregation with name: " + name);
    }

    @Override
    public Double getScore(Object id) {
        if (parameters.getScoringStrategy() != null) {
            VertexiumObject vertexiumObject = findVertexiumObjectById(id);
            if (vertexiumObject != null) {
                return parameters.getScoringStrategy().getScore(vertexiumObject);
            }
        }
        return 0.0;
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return 0;
    }

    private VertexiumObject findVertexiumObjectById(Object id) {
        Iterator<T> it = iterator(true);
        while (it.hasNext()) {
            T obj = it.next();
            if (obj instanceof VertexiumObject) {
                VertexiumObject vertexiumObject = (VertexiumObject) obj;
                if (vertexiumObject.getId().equals(id)) {
                    return vertexiumObject;
                }
            }
        }
        return null;
    }
}
