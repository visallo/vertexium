package org.vertexium.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class GroupingIterable<TSource, TGroup> implements CloseableIterable<TGroup> {
    private boolean doneCalled;
    private final Iterable<TSource> source;

    public GroupingIterable(Iterable<TSource> source) {
        this.source = source;
    }

    @Override
    public Iterator<TGroup> iterator() {
        Iterator<TSource> it = source.iterator();
        return new CloseableIterator<TGroup>() {
            private TGroup next;
            private TGroup current;
            private TSource lastItem;

            @Override
            public boolean hasNext() {
                loadNext();
                if (next == null) {
                    close();
                }
                return next != null;
            }

            @Override
            public TGroup next() {
                loadNext();
                if (next == null) {
                    throw new NoSuchElementException();
                }
                this.current = this.next;
                this.next = null;
                return this.current;
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

                if (lastItem != null) {
                    this.next = createGroup(lastItem);
                    lastItem = null;
                }

                while (it.hasNext()) {
                    TSource item = it.next();
                    if (!isIncluded(item)) {
                        continue;
                    }
                    if (this.next == null) {
                        this.next = createGroup(item);
                    } else if (isPartOfGroup(this.next, item)) {
                        addToGroup(this.next, item);
                    } else {
                        lastItem = item;
                        break;
                    }
                }
            }
        };
    }

    protected boolean isIncluded(TSource item) {
        return true;
    }

    protected abstract TGroup createGroup(TSource item);

    protected abstract boolean isPartOfGroup(TGroup group, TSource item);

    protected abstract void addToGroup(TGroup group, TSource item);

    private void callClose() {
        if (!doneCalled) {
            doneCalled = true;
            close();
        }
    }

    @Override
    public void close() {

    }
}
