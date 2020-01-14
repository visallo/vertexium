package org.vertexium.elasticsearch7.bulk;

import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;
import org.vertexium.util.VertexiumReadWriteLock;
import org.vertexium.util.VertexiumStampedLock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class BulkItemBatch {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkItemBatch.class);
    private final VertexiumReadWriteLock lock = new VertexiumStampedLock();
    private final int maxBatchSize;
    private final int maxBatchSizeInBytes;
    private final long batchWindowTimeMillis;
    private final Integer logRequestSizeLimit;
    private long lastFlush;
    private LinkedHashMap<String, BulkItem<?>> batch = new LinkedHashMap<>();
    private int currentBatchSizeInBytes = 0;

    public BulkItemBatch(
        int maxBatchSize,
        int maxBatchSizeInBytes,
        Duration batchWindowTime,
        Integer logRequestSizeLimit
    ) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.batchWindowTimeMillis = batchWindowTime.toMillis();
        this.logRequestSizeLimit = logRequestSizeLimit;
        this.lastFlush = System.currentTimeMillis();
    }

    public boolean add(Item item) {
        return lock.executeInWriteLock(() -> {
            String batchKey = getBatchKey(item);
            BulkItem<?> bulkItem = batch.get(batchKey);
            if (!canAdd(item)) {
                return false;
            }

            if (bulkItem != null) {
                // subtract the old size, after we add this item we need to add the new size back in
                currentBatchSizeInBytes -= bulkItem.getSize();
            } else {
                if (item instanceof DeleteItem) {
                    bulkItem = new BulkDeleteItem(
                        item.getIndexName(),
                        item.getType(),
                        item.getDocumentId(),
                        item.getVertexiumObjectId()
                    );
                } else if (item instanceof UpdateItem) {
                    bulkItem = new BulkUpdateItem(
                        item.getIndexName(),
                        item.getType(),
                        item.getDocumentId(),
                        item.getVertexiumObjectId(),
                        ((UpdateItem) item).getSourceElementLocation()
                    );
                } else {
                    throw new VertexiumException("Unhandled item type: " + item.getClass().getName());
                }
                batch.put(batchKey, bulkItem);
            }

            addToBulkItemUnsafe(bulkItem, item);
            logRequestSize(item);
            currentBatchSizeInBytes += bulkItem.getSize();
            return true;
        });
    }

    private String getBatchKey(Item item) {
        return String.format("%s:%s:%s:%s", item.getIndexName(), item.getType(), item.getDocumentId(), item.getClass().getName());
    }

    private boolean canAdd(Item item) {
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addToBulkItemUnsafe(BulkItem<?> bulkItem, Item item) {
        ((BulkItem) bulkItem).add(item);
    }

    private void logRequestSize(Item item) {
        if (logRequestSizeLimit == null) {
            return;
        }
        int sizeInBytes = item.getSize();
        if (sizeInBytes > logRequestSizeLimit) {
            LOGGER.warn("Large document detected (id: %s). Size in bytes: %d", item.getVertexiumObjectId(), sizeInBytes);
        }
    }

    public boolean shouldFlushByTime() {
        return lock.executeInReadLock(() ->
            batch.size() > 0 && ((System.currentTimeMillis() - lastFlush) > batchWindowTimeMillis)
        );
    }

    public int size() {
        return batch.size();
    }

    public List<BulkItem<?>> getItemsAndClear() {
        return lock.executeInWriteLock(() -> {
            List<BulkItem<?>> results = new ArrayList<>(batch.values());
            batch = new LinkedHashMap<>();
            currentBatchSizeInBytes = 0;
            lastFlush = System.currentTimeMillis();
            return results;
        });
    }
}
