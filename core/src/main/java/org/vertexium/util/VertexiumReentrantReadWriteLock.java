package org.vertexium.util;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class VertexiumReentrantReadWriteLock {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ReadWriteLock getLock() {
        return lock;
    }

    public <T> T executeInReadLock(Supplier<T> fn) {
        lock.readLock().lock();
        try {
            return fn.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void executeInReadLock(Runnable fn) {
        lock.readLock().lock();
        try {
            fn.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    public <T> T executeInWriteLock(Supplier<T> fn) {
        lock.writeLock().lock();
        try {
            return fn.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void executeInWriteLock(Runnable fn) {
        lock.writeLock().lock();
        try {
            fn.run();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
