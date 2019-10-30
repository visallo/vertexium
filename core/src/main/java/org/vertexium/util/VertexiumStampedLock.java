package org.vertexium.util;

import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

public class VertexiumStampedLock implements VertexiumReadWriteLock {
    private final StampedLock lock = new StampedLock();

    public StampedLock getLock() {
        return lock;
    }

    @Override
    public <T> T executeInReadLock(Supplier<T> fn) {
        long stamp = lock.readLock();
        try {
            return fn.get();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void executeInReadLock(Runnable fn) {
        long stamp = lock.readLock();
        try {
            fn.run();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public <T> T executeInWriteLock(Supplier<T> fn) {
        long stamp = lock.writeLock();
        try {
            return fn.get();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void executeInWriteLock(Runnable fn) {
        long stamp = lock.writeLock();
        try {
            fn.run();
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
