package org.vertexium.metric;

public interface Counter {
    default void increment() {
        increment(1);
    }

    void increment(long amount);

    default void decrement() {
        decrement(1);
    }

    void decrement(long i);

    long getCount();
}
