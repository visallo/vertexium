package org.vertexium.elasticsearch7.bulk;

import org.vertexium.util.VertexiumReadWriteLock;
import org.vertexium.util.VertexiumStampedLock;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class OutstandingItemsList {
    private final LinkedList<BulkItem> outstandingItems = new LinkedList<>();
    private final VertexiumReadWriteLock lock = new VertexiumStampedLock();

    public void add(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> {
            outstandingItems.add(bulkItem);
        });
    }

    public List<CompletableFuture<Void>> getFutures() {
        return lock.executeInReadLock(() ->
            outstandingItems.stream()
                .map(BulkItem::getFuture)
                .collect(Collectors.toList())
        );
    }

    public BulkItemCompletableFuture getFutureForElementId(String elementId) {
        return lock.executeInReadLock(() ->
            outstandingItems.stream()
                .filter(item -> elementId.equals(item.getElementId().getId()))
                .map(BulkItem::getFuture)
                .findFirst().orElse(null)
        );
    }

    public int size() {
        return outstandingItems.size();
    }

    public void remove(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> {
            outstandingItems.remove(bulkItem);
        });
    }
}
