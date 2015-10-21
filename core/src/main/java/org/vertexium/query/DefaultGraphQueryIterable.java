package org.vertexium.query;

import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.util.CloseableIterable;
import org.vertexium.util.CloseableIterator;
import org.vertexium.util.CloseableUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;

public class DefaultGraphQueryIterable<T extends Element> implements
        Iterable<T>,
        QueryResultsIterable<T> {
    private final QueryParameters parameters;
    private final Iterable<T> iterable;
    private final boolean evaluateQueryString;
    private final boolean evaluateHasContainers;

    public DefaultGraphQueryIterable(
            QueryParameters parameters,
            Iterable<T> iterable,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers
    ) {
        this.parameters = parameters;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        if (evaluateSortContainers && this.parameters.getSortContainers().size() > 0) {
            this.iterable = sort(iterable, parameters.getSortContainers());
        } else {
            this.iterable = iterable;
        }
    }

    private Iterable<T> sort(Iterable<T> iterable, List<QueryBase.SortContainer> sortContainers) {
        List<T> list = toList(iterable);
        Collections.sort(list, new SortContainersComparator<T>(sortContainers));
        return list;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(false);
    }

    private Iterator<T> iterator(final boolean iterateAll) {
        final Iterator<T> it = iterable.iterator();

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
                DefaultGraphQueryIterable.this.close();
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

                    boolean match = true;
                    if (evaluateHasContainers) {
                        for (QueryBase.HasContainer has : parameters.getHasContainers()) {
                            if (!has.isMatch(elem)) {
                                match = false;
                                break;
                            }
                        }
                        if (elem instanceof Edge && parameters.getEdgeLabels().size() > 0) {
                            Edge edge = (Edge) elem;
                            if (!parameters.getEdgeLabels().contains(edge.getLabel())) {
                                match = false;
                            }
                        }
                    }
                    if (!match) {
                        continue;
                    }
                    if (evaluateQueryString
                            && parameters instanceof QueryStringQueryParameters
                            && ((QueryStringQueryParameters) parameters).getQueryString() != null
                            && !evaluateQueryString(elem, ((QueryStringQueryParameters) parameters).getQueryString())
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

    protected boolean evaluateQueryString(Element elem, String queryString) {
        for (Property property : elem.getProperties()) {
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
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }

    @Override
    public long getTotalHits() {
        // a limit could be set on a query which could prevent all items being returned
        return count(this.iterator(true));
    }

    @Override
    public void close() {
        CloseableUtils.closeQuietly(iterable);
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        throw new VertexiumException("Could not find aggregation with name: " + name);
    }
}
