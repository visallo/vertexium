package org.vertexium.elasticsearch5.bulk;

import java.util.concurrent.ConcurrentLinkedQueue;

public class FailureList {
    private final ConcurrentLinkedQueue<BulkItemFailure> failures = new ConcurrentLinkedQueue<>();

    public void add(BulkItemFailure failure) {
        failures.add(failure);
    }

    public void remove(BulkItemFailure failure) {
        failures.remove(failure);
    }

    public BulkItemFailure peek() {
        return failures.peek();
    }

    public int size() {
        return failures.size();
    }

    public long size(long beforeTime) {
        return failures.stream()
            .filter(failure -> failure.getBulkItem().getCreatedTime() <= beforeTime)
            .count();
    }
}
