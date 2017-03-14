package org.vertexium.util;

import java.util.Iterator;

public class OffsetAndSizeIterable<T> implements CloseableIterable<T> {
    private boolean doneCalled;
    private final Iterable<T> iterable;
    private final int offset;
    private final Integer size;

    public OffsetAndSizeIterable(Iterable<T> iterable, int offset, Integer size) {
        this.iterable = iterable;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = iterable.iterator();

        return new CloseableIterator<T>() {
            private int currentOffset;
            private T next;
            private T current;

            @Override
            public boolean hasNext() {
                loadNext();
                if (next == null) {
                    close();
                }
                return next != null;
            }

            @Override
            public T next() {
                loadNext();
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

                while (it.hasNext()) {
                    T n = it.next();
                    if (isInRange()) {
                        currentOffset++;
                        this.next = n;
                        break;
                    } else {
                        currentOffset++;
                    }
                }
            }

            private boolean isInRange() {
                return currentOffset >= offset && (size == null || currentOffset < offset + size);
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
}
