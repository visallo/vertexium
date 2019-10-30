package org.vertexium.util;

import java.util.function.Supplier;

public interface VertexiumReadWriteLock {
    <T> T executeInReadLock(Supplier<T> fn);

    void executeInReadLock(Runnable fn);

    <T> T executeInWriteLock(Supplier<T> fn);

    void executeInWriteLock(Runnable fn);
}
