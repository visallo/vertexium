package org.vertexium.elasticsearch5.bulk;

import org.vertexium.VertexiumException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PendingFuturesList {
    private final ConcurrentLinkedQueue<Item> pendingFutures = new ConcurrentLinkedQueue<>();

    public void remove(CompletableFuture<FlushBatchResult> future) {
        pendingFutures.remove(new Item(null, future));
    }

    public void add(List<BulkItem> batch, CompletableFuture<FlushBatchResult> future) {
        pendingFutures.add(new Item(batch, future));
    }

    public CompletableFuture<FlushBatchResult> peek() {
        Item result = pendingFutures.peek();
        if (result == null) {
            return null;
        }
        return result.future;
    }

    public long size(long beforeTime) {
        return pendingFutures.stream()
            .filter(item -> item.isBeforeTime(beforeTime))
            .count();
    }

    private static class Item {
        public final List<BulkItem> batch;
        public final CompletableFuture<FlushBatchResult> future;

        public Item(List<BulkItem> batch, CompletableFuture<FlushBatchResult> future) {
            if (future == null) {
                throw new VertexiumException("future cannot be null");
            }
            this.batch = batch;
            this.future = future;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Item item = (Item) o;
            return Objects.equals(future, item.future);
        }

        @Override
        public int hashCode() {
            return Objects.hash(future);
        }

        public boolean isBeforeTime(long beforeTime) {
            return batch.stream()
                .anyMatch(item -> item.getCreatedTime() <= beforeTime);
        }
    }
}
