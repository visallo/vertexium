package org.vertexium.metric;

public interface Counter {
    void increment();

    void decrement();

    void getCount();
}
