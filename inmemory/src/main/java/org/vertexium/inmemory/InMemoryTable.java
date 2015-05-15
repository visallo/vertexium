package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.FetchHint;
import org.vertexium.inmemory.mutations.Mutation;
import org.vertexium.util.LookAheadIterable;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

abstract class InMemoryTable<TElement extends InMemoryElement> {
    private ConcurrentSkipListMap<String, InMemoryTableElement<TElement>> rows = new ConcurrentSkipListMap<>();

    public TElement get(InMemoryGraph graph, String id, Authorizations authorizations) {
        InMemoryTableElement<TElement> inMemoryTableElement = getTableElement(id);
        if (inMemoryTableElement == null) {
            return null;
        }
        return inMemoryTableElement.createElement(graph, authorizations);
    }

    public InMemoryTableElement<TElement> getTableElement(String id) {
        return rows.get(id);
    }

    public synchronized void append(String id, Mutation... newMutations) {
        InMemoryTableElement<TElement> inMemoryTableElement = rows.get(id);
        if (inMemoryTableElement == null) {
            inMemoryTableElement = createInMemoryTableElement(id);
            rows.put(id, inMemoryTableElement);
        }
        inMemoryTableElement.addAll(newMutations);
    }

    protected abstract InMemoryTableElement<TElement> createInMemoryTableElement(String id);

    public void remove(String id) {
        rows.remove(id);
    }

    public void clear() {
        rows.clear();
    }

    public Iterable<TElement> getAll(final InMemoryGraph graph, EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);
        return new LookAheadIterable<InMemoryTableElement<TElement>, TElement>() {
            @Override
            protected boolean isIncluded(InMemoryTableElement<TElement> src, TElement element) {
                return isIncludedInTimeSpan(src, includeHidden, endTime, authorizations);
            }

            @Override
            protected TElement convert(InMemoryTableElement<TElement> element) {
                return element.createElement(graph, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableElement<TElement>> createIterator() {
                return rows.values().iterator();
            }
        };
    }

    private boolean isIncludedInTimeSpan(InMemoryTableElement src, boolean includeHidden, Long endTime, Authorizations authorizations) {
        if (!src.canRead(authorizations)) {
            return false;
        }

        if (!includeHidden && src.isHidden(authorizations)) {
            return false;
        }

        if (endTime != null && src.getFirstTimestamp() > endTime) {
            return false;
        }

        return true;
    }

    public Iterable<InMemoryTableElement<TElement>> getRowValues() {
        return this.rows.values();
    }
}
