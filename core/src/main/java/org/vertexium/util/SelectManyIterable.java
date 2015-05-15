package org.vertexium.util;

import java.util.Iterator;

public abstract class SelectManyIterable<TSource, TDest> implements Iterable<TDest> {
    private final Iterable<? extends TSource> source;

    public SelectManyIterable(Iterable<? extends TSource> source) {
        this.source = source;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<? extends TSource> it = source.iterator();
        return new Iterator<TDest>() {
            private TDest next;
            private TDest current;
            public Iterator<? extends TDest> innerIterator = null;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public TDest next() {
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
                while (true) {
                    if (innerIterator != null) {
                        if (innerIterator.hasNext()) {
                            TDest dest = innerIterator.next();
                            if (isIncluded(dest)) {
                                this.next = dest;
                                return;
                            }
                        } else {
                            innerIterator = null;
                        }
                    } else {
                        if (it.hasNext()) {
                            TSource nextSource = it.next();
                            innerIterator = getIterable(nextSource).iterator();
                        } else {
                            return;
                        }
                    }
                }
            }
        };
    }

    protected boolean isIncluded(TDest dest) {
        return true;
    }

    protected abstract Iterable<? extends TDest> getIterable(TSource source);
}
