package org.vertexium.metric;

import java.util.function.Supplier;

public interface VertexiumMetricRegistry {
    String createName(Class clazz, String... nameParts);

    default Counter getCounter(Class clazz, String... nameParts) {
        return getCounter(createName(clazz, nameParts));
    }

    Counter getCounter(String name);

    Iterable<? extends Counter> getCounters();

    default Timer getTimer(Class clazz, String... nameParts) {
        return getTimer(createName(clazz, nameParts));
    }

    Timer getTimer(String name);

    Iterable<? extends Timer> getTimers();

    default Histogram getHistogram(Class clazz, String... nameParts) {
        return getHistogram(createName(clazz, nameParts));
    }

    Histogram getHistogram(String name);

    Iterable<? extends Histogram> getHistograms();

    default <T> Gauge<T> getGauge(Class clazz, String namePart1, Supplier<T> supplier) {
        return getGauge(createName(clazz, namePart1), supplier);
    }

    default <T> Gauge<T> getGauge(Class clazz, String namePart1, String namePart2, Supplier<T> supplier) {
        return getGauge(createName(clazz, namePart1, namePart2), supplier);
    }

    <T> Gauge<T> getGauge(String name, Supplier<T> supplier);

    Iterable<? extends Gauge> getGauges();

    default StackTraceTracker getStackTraceTracker(Class clazz, String... nameParts) {
        return getStackTraceTracker(createName(clazz, nameParts));
    }

    StackTraceTracker getStackTraceTracker(String name);

    Iterable<? extends StackTraceTracker> getStackTraceTrackers();
}
