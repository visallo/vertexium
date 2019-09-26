package org.vertexium.metric;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

public class DropWizardMetricRegistry implements VertexiumMetricRegistry {
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Override
    public Counter getCounter(Class clazz, String... nameParts) {
        String name = createName(clazz, nameParts);
        com.codahale.metrics.Counter counter = metricRegistry.counter(name);
        return new Counter() {
            @Override
            public void increment() {
                counter.inc();
            }

            @Override
            public void decrement() {
                counter.dec();
            }

            @Override
            public void getCount() {
                counter.getCount();
            }
        };
    }

    @Override
    public Timer getTimer(Class clazz, String... nameParts) {
        String name = createName(clazz, nameParts);
        com.codahale.metrics.Timer timer = metricRegistry.timer(name);
        return new Timer() {
            @Override
            public <T> T time(Supplier<T> supplier) {
                com.codahale.metrics.Timer.Context ctx = timer.time();
                try {
                    return supplier.get();
                } finally {
                    ctx.stop();
                }
            }
        };
    }

    @Override
    public String createName(Class clazz, String... nameParts) {
        return MetricRegistry.name(clazz, nameParts);
    }

    @Override
    public <T> void registerGauage(String name, Supplier<T> supplier) {
        metricRegistry.register(name, (Gauge<T>) supplier::get);
    }
}
