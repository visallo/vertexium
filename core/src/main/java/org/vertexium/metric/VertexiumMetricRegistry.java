package org.vertexium.metric;

import java.util.function.Supplier;

public interface VertexiumMetricRegistry {
    Counter getCounter(Class clazz, String... nameParts);

    Timer getTimer(Class clazz, String... nameParts);

    String createName(Class clazz, String... nameParts);

    <T> void registerGauage(String name, Supplier<T> supplier);
}
