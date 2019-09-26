package org.vertexium.metric;

import java.util.function.Supplier;

public interface Timer {
    default void time(Runnable runnable) {
        time(() -> {
            runnable.run();
            return null;
        });
    }

    <T> T time(Supplier<T> supplier);
}
