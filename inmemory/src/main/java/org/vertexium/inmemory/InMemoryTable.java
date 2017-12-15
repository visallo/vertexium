package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.FetchHint;
import org.vertexium.inmemory.mutations.Mutation;
import org.vertexium.util.LookAheadIterable;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public abstract class InMemoryTable<TElement extends InMemoryElement> {
    private Map<String, InMemoryTableElement<TElement>> rows;

    protected InMemoryTable(Map<String, InMemoryTableElement<TElement>> rows) {
        this.rows = rows;
    }

    protected InMemoryTable() {
        this(new ConcurrentSkipListMap<String, InMemoryTableElement<TElement>>());
    }

    public TElement get(InMemoryGraph graph, String id, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        InMemoryTableElement<TElement> inMemoryTableElement = getTableElement(id);
        if (inMemoryTableElement == null) {
            return null;
        }
        return inMemoryTableElement.createElement(graph, fetchHints, authorizations);
    }

    public InMemoryTableElement<TElement> getTableElement(String id) {
        return rows.get(id);
    }

    public synchronized void append(String id, Mutation... newMutations) {
        InMemoryTableElement<TElement> inMemoryTableElement = rows.get(id);
        if (inMemoryTableElement == null) {
            inMemoryTableElement = createInMemoryTableElement(id);
            rows.put(id, inMemoryTableElement);
            // Sort rows by id in ascending order
            rows = rows.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
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

    public Iterable<TElement> getAll(
            InMemoryGraph graph,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        return new LookAheadIterable<InMemoryTableElement<TElement>, TElement>() {
            @Override
            protected boolean isIncluded(InMemoryTableElement<TElement> src, TElement element) {
                return graph.isIncludedInTimeSpan(src, fetchHints, endTime, authorizations);
            }

            @Override
            protected TElement convert(InMemoryTableElement<TElement> element) {
                return element.createElement(graph, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableElement<TElement>> createIterator() {
                return rows.values().iterator();
            }
        };
    }

    public Iterable<InMemoryTableElement<TElement>> getRowValues() {
        return this.rows.values();
    }
}
