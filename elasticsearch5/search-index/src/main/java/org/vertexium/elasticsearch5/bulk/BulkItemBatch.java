package org.vertexium.elasticsearch5.bulk;

import org.vertexium.util.VertexiumReadWriteLock;
import org.vertexium.util.VertexiumStampedLock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BulkItemBatch {
    private final VertexiumReadWriteLock lock = new VertexiumStampedLock();
    private final int maxBatchSize;
    private final int maxBatchSizeInBytes;
    private final long batchWindowTimeMillis;
    private long lastFlush;
    private List<BulkItem> batch = new ArrayList<>();
    private int currentBatchSizeInBytes = 0;

    public BulkItemBatch(int maxBatchSize, int maxBatchSizeInBytes, Duration batchWindowTime) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.batchWindowTimeMillis = batchWindowTime.toMillis();
        this.lastFlush = System.currentTimeMillis();
    }

    public boolean add(BulkItem item) {
        return lock.executeInWriteLock(() -> {
            if (canAdd(item)) {
                batch.add(item);
                currentBatchSizeInBytes += item.getSize();
                return true;
            }
            return false;
        });
    }

    private boolean canAdd(BulkItem item) {
        if (batch.size() == 0) {
            return true;
        }

        if (batch.size() >= maxBatchSize) {
            return false;
        }

        if (currentBatchSizeInBytes + item.getSize() >= maxBatchSizeInBytes) {
            return false;
        }

        return true;
    }

    public List<BulkItem> getItemsAndClear() {
        return lock.executeInWriteLock(() -> {
            List<BulkItem> results = batch;
            batch = new ArrayList<>();
            currentBatchSizeInBytes = 0;
            lastFlush = System.currentTimeMillis();
            return results;
        });
    }

    public int size() {
        return batch.size();
    }

    public boolean shouldFlushByTime() {
        return lock.executeInReadLock(() ->
            batch.size() > 0 && ((System.currentTimeMillis() - lastFlush) > batchWindowTimeMillis)
        );
    }
}
