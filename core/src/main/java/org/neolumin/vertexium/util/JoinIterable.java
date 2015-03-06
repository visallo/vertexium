package org.neolumin.vertexium.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class JoinIterable<T> implements Iterable<T> {
    private final Iterable<T>[] iterables;

    @SafeVarargs
    public JoinIterable(Iterable<T>... iterables) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<T> iterator() {
        if (this.iterables.length == 0) {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public T next() {
                    return null;
                }

                @Override
                public void remove() {

                }
            };
        }

        final Queue<Iterable<T>> iterables = new LinkedList<>();
        Collections.addAll(iterables, this.iterables);
        final IteratorWrapper it = new IteratorWrapper();
        it.iterator = iterables.remove().iterator();

        return new Iterator<T>() {
            private T next;
            private T current;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public T next() {
                loadNext();
                if (this.next == null) {
                    throw new IllegalStateException("iterable doesn't have a next element");
                }
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                it.iterator.remove();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                while (true) {
                    if (it.iterator.hasNext()) {
                        break;
                    }
                    if (iterables.size() == 0) {
                        this.next = null;
                        return;
                    }
                    it.iterator = iterables.remove().iterator();
                }
                this.next = it.iterator.next();
            }
        };
    }

    private class IteratorWrapper {
        public Iterator<T> iterator;
    }
}
