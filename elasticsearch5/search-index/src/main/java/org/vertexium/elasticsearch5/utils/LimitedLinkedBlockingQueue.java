package org.vertexium.elasticsearch5.utils;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class LimitedLinkedBlockingQueue<T> extends LinkedBlockingQueue<T> {
    public LimitedLinkedBlockingQueue() {
    }

    public LimitedLinkedBlockingQueue(Collection<? extends T> c) {
        super(c);
    }

    public LimitedLinkedBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(T t) {
        try {
            put(t);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
