package org.neolumin.vertexium.util;

import java.util.Iterator;

public abstract class ConvertingIterable<TSource, TDest> implements Iterable<TDest> {
    private final Iterable<TSource> iterable;

    public ConvertingIterable(Iterable<TSource> iterable) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<TSource> it = iterable.iterator();
        return new Iterator<TDest>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public TDest next() {
                return convert(it.next());
            }

            @Override
            public void remove() {
                it.remove();
            }
        };
    }

    protected abstract TDest convert(TSource o);
}
