package org.neolumin.vertexium.util;

import java.util.Iterator;

public class ArrayIterable<T> implements Iterable<T> {
    private final T[] items;

    public ArrayIterable(T[] items) {
        this.items = items;
    }

    @Override
    public Iterator<T> iterator() {
        return new ArrayIterator(this.items);
    }
}
