package org.vertexium.metric;

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class NullMetricRegistry implements VertexiumMetricRegistry {
    private final Map<String, Counter> countersByName = new HashMap<>();
    private final Map<String, Timer> timersByName = new HashMap<>();
    private final Map<String, Histogram> histogramsByName = new HashMap<>();
    private final Map<String, Gauge> gaugesByName = new HashMap<>();
    private final Map<String, StackTraceTracker> stackTraceTrackersByName = new HashMap<>();

    @Override
    public String createName(Class clazz, String... nameParts) {
        return clazz.getName() + "." + Joiner.on(".").join(nameParts);
    }

    @Override
    public Counter getCounter(String name) {
        return countersByName.computeIfAbsent(name, s -> new Counter() {
            @Override
            public void increment() {
            }

            @Override
            public void decrement() {
            }

            @Override
            public void getCount() {
            }
        });
    }

    @Override
    public Iterable<Counter> getCounters() {
        return countersByName.values();
    }

    @Override
    public Timer getTimer(String name) {
        return timersByName.computeIfAbsent(name, s -> new Timer() {
            @Override
            public <T> T time(Supplier<T> supplier) {
                return supplier.get();
            }
        });
    }

    @Override
    public Iterable<Timer> getTimers() {
        return timersByName.values();
    }

    @Override
    public Histogram getHistogram(String name) {
        return histogramsByName.computeIfAbsent(name, s -> new Histogram() {
            @Override
            public void update(int value) {
            }

            @Override
            public void update(long value) {
            }
        });
    }

    @Override
    public Iterable<Histogram> getHistograms() {
        return histogramsByName.values();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Gauge<T> getGauge(String name, Supplier<T> supplier) {
        return gaugesByName.computeIfAbsent(name, s -> new Gauge<T>() {
        });
    }

    @Override
    public Iterable<Gauge> getGauges() {
        return gaugesByName.values();
    }

    @Override
    public StackTraceTracker getStackTraceTracker(String name) {
        return stackTraceTrackersByName.computeIfAbsent(name, s -> new StackTraceTracker());
    }

    @Override
    public Iterable<? extends StackTraceTracker> getStackTraceTrackers() {
        return stackTraceTrackersByName.values();
    }
}
