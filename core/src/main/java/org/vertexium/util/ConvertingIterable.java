package org.vertexium.util;

import java.util.Iterator;

public abstract class ConvertingIterable<TSource, TDest> implements Iterable<TDest> {
    private final Iterable<? extends TSource> iterable;

    public ConvertingIterable(Iterable<? extends TSource> iterable) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<? extends TSource> it = iterable.iterator();
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
