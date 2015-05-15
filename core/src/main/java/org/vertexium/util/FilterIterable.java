package org.vertexium.util;

import java.util.Iterator;

public abstract class FilterIterable<T> extends LookAheadIterable<T, T> implements Iterable<T> {
    private final Iterable<? extends T> iterable;

    public FilterIterable(Iterable<? extends T> iterable) {
        this.iterable = iterable;
    }

    @Override
    protected T convert(T next) {
        return next;
    }

    @Override
    protected Iterator<T> createIterator() {
        return (Iterator<T>) this.iterable.iterator();
    }

    protected final boolean isIncluded(T src, T dest) {
        return isIncluded(src);
    }

    protected abstract boolean isIncluded(T o);
}
