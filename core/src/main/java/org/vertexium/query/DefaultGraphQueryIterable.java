package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.util.CloseableIterable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;

public class DefaultGraphQueryIterable<T extends Element> implements
        Iterable<T>,
        IterableWithTotalHits<T>,
        CloseableIterable<T> {
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

        return new Iterator<T>() {
            public T next;
            public T current;
            public long count;

            @Override
            public boolean hasNext() {
                loadNext();
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

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                if (!iterateAll && (this.count >= parameters.getSkip() + parameters.getLimit())) {
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
        return count(this.iterator(true));
    }

    @Override
    public void close() throws IOException {
        if (this.iterable instanceof CloseableIterable) {
            ((CloseableIterable) this.iterable).close();
        }
    }
}
