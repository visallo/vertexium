package org.vertexium.util;

import java.util.concurrent.CompletableFuture;

import static org.vertexium.util.IterableUtils.toArray;

public class CompletableFutureUtils {
    public static CompletableFuture<Void> allOf(Iterable<? extends CompletableFuture<?>> futures) {
        CompletableFuture<?>[] futuresArray = toArray(futures, CompletableFuture.class);
        return CompletableFuture.allOf(futuresArray);
    }

    public static CompletableFuture<Object[]> allOfWithResults(Iterable<? extends CompletableFuture<?>> futures) {
        CompletableFuture<?>[] futuresArray = toArray(futures, CompletableFuture.class);
        return CompletableFuture.allOf(futuresArray)
            .thenApply((_void) -> {
                Object[] results = new Object[futuresArray.length];
                for (int i = 0; i < futuresArray.length; i++) {
                    results[i] = futuresArray[i].join();
                }
                return results;
            });
    }
}
