package org.neolumin.vertexium.util;

import java.util.Iterator;

public class ArrayIterator<T> implements Iterator<T> {
    private final T[] items;
    private int index;

    public ArrayIterator(T[] items) {
        this.items = items;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return this.index < this.items.length;
    }

    @Override
    public T next() {
        return this.items[this.index++];
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }
}
