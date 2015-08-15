package org.vertexium.util;

import java.util.Iterator;

public abstract class LookAheadIterable<TSource, TDest> implements CloseableIterable<TDest> {
    private boolean doneCalled;

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<TSource> it = createIterator();

        return new CloseableIterator<TDest>() {
            private TDest next;
            private TDest current;

            @Override
            public boolean hasNext() {
                loadNext();
                if (next == null) {
                    close();
                }
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

            @Override
            public void close() {
                CloseableUtils.closeQuietly(it);
                callClose();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                while (it.hasNext()) {
                    TSource n = it.next();
                    TDest obj = convert(n);
                    if (!isIncluded(n, obj)) {
                        continue;
                    }

                    this.next = obj;
                    break;
                }
            }
        };
    }

    private void callClose() {
        if (!doneCalled) {
            doneCalled = true;
            close();
        }
    }

    @Override
    public void close() {

    }

    protected abstract boolean isIncluded(TSource src, TDest dest);

    protected abstract TDest convert(TSource next);

    protected abstract Iterator<TSource> createIterator();
}
