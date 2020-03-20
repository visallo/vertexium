package org.vertexium.util;

import java.util.Iterator;

public class ClosingIterator<T> implements Iterator<T> {
    private final Iterator<T> it;
    private final Runnable onClose;
    private boolean closeCalled;

    public ClosingIterator(Iterator<T> it, Runnable onClose) {
        this.it = it;
        this.onClose = onClose;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = it.hasNext();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public T next() {
        return it.next();
    }

    @Override
    public void remove() {
        it.remove();
    }

    private void close() {
        if (closeCalled) {
            return;
        }
        onClose.run();
    }
}
