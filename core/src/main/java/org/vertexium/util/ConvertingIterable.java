package org.vertexium.util;

import java.util.Iterator;

public abstract class ConvertingIterable<TSource, TDest> implements Iterable<TDest> {
    private Iterable<? extends TSource> iterable;
    private Iterator<? extends TSource> iterator;

    public ConvertingIterable(Iterable<? extends TSource> iterable) {
        this.iterable = iterable;
    }

    public ConvertingIterable(Iterator<? extends TSource> iterator) {
        this.iterator = iterator;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<? extends TSource> it = iterator == null ? iterable.iterator() : iterator;
        return new CloseableIterator<TDest>() {
            @Override
            public boolean hasNext() {
                boolean hasNext = it.hasNext();
                if (!hasNext) {
                    close();
                }
                return hasNext;
            }

            @Override
            public TDest next() {
                return convert(it.next());
            }

            @Override
            public void remove() {
                it.remove();
            }

            @Override
            public void close() {
                CloseableUtils.closeQuietly(it);
            }
        };
    }

    protected abstract TDest convert(TSource o);
}
