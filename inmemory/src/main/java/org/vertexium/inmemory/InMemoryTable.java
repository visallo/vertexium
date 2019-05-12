package org.vertexium.inmemory;

import org.vertexium.FetchHints;
import org.vertexium.User;
import org.vertexium.inmemory.mutations.Mutation;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public abstract class InMemoryTable<TElement extends InMemoryElement> {
    private ReadWriteLock rowsLock = new ReentrantReadWriteLock();
    private Map<String, InMemoryTableElement<TElement>> rows;

    protected InMemoryTable(Map<String, InMemoryTableElement<TElement>> rows) {
        this.rows = rows;
    }

    protected InMemoryTable() {
        this(new ConcurrentSkipListMap<>());
    }

    public TElement get(InMemoryGraph graph, String id, FetchHints fetchHints, User user) {
        InMemoryTableElement<TElement> inMemoryTableElement = getTableElement(id);
        if (inMemoryTableElement == null) {
            return null;
        }
        return inMemoryTableElement.createElement(graph, fetchHints, user);
    }

    public InMemoryTableElement<TElement> getTableElement(String id) {
        rowsLock.readLock().lock();
        try {
            return rows.get(id);
        } finally {
            rowsLock.readLock().unlock();
        }
    }

    public void append(String id, Mutation... newMutations) {
        rowsLock.writeLock().lock();
        try {
            InMemoryTableElement<TElement> inMemoryTableElement = rows.get(id);
            if (inMemoryTableElement == null) {
                inMemoryTableElement = createInMemoryTableElement(id);
                rows.put(id, inMemoryTableElement);
            }
            inMemoryTableElement.addAll(newMutations);
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    protected abstract InMemoryTableElement<TElement> createInMemoryTableElement(String id);

    public void remove(String id) {
        rowsLock.writeLock().lock();
        try {
            rows.remove(id);
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    public void clear() {
        rowsLock.writeLock().lock();
        try {
            rows.clear();
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    public Stream<TElement> getAll(
        InMemoryGraph graph,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        return getRowValues()
            .filter(element -> graph.isIncludedInTimeSpan(element, fetchHints, endTime, user))
            .map(element -> element.createElement(graph, fetchHints, endTime, user));
    }

    public Stream<InMemoryTableElement<TElement>> getRowValues() {
        rowsLock.readLock().lock();
        try {
            return new ArrayList<>(this.rows.values()).stream();
        } finally {
            rowsLock.readLock().unlock();
        }
    }
}
