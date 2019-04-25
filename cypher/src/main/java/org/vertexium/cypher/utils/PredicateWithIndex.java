package org.vertexium.cypher.utils;

import java.util.function.Predicate;

public abstract class PredicateWithIndex<T> implements Predicate<T> {
    private long index;

    @Override
    public boolean test(T t) {
        return test(t, index++);
    }

    protected abstract boolean test(T t, long index);
}
