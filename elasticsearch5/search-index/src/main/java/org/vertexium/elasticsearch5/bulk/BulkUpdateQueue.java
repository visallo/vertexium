package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.IndexRefreshTracker;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;
import org.vertexium.util.VertexiumReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BulkUpdateQueue {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateQueue.class);
    private final IndexRefreshTracker indexRefreshTracker;
    private final BulkUpdateService bulkUpdateService;
    private final BulkItemList todoItems = new BulkItemList();
    private final BulkItemList submittedItems = new BulkItemList();
    private final PendingFuturesList pendingFutures = new PendingFuturesList();
    private final FailureList failures = new FailureList();
    private final VertexiumReentrantReadWriteLock lock = new VertexiumReentrantReadWriteLock();
    private final int maxBatchSize;
    private final int maxBatchSizeInBytes;
    private final int maxFailCount;

    public BulkUpdateQueue(
        IndexRefreshTracker indexRefreshTracker,
        BulkUpdateService bulkUpdateService,
        BulkUpdateQueueConfiguration configuration
    ) {
        this.indexRefreshTracker = indexRefreshTracker;
        this.bulkUpdateService = bulkUpdateService;
        this.maxBatchSize = configuration.getMaxBatchSize();
        this.maxBatchSizeInBytes = configuration.getMaxBatchSizeInBytes();
        this.maxFailCount = configuration.getMaxFailCount();
    }

    public boolean containsElementId(String elementId) {
        return lock.executeInReadLock(() -> {
            return todoItems.containsElementId(elementId) || submittedItems.containsElementId(elementId);
        });
    }

    public Long getOldestTodoItemTime() {
        return lock.executeInReadLock(todoItems::getOldestItemTime);
    }

    public CompletableFuture<FlushBatchResult> flushSingleBatch() {
        List<BulkItem> batch = getBatch();
        if (batch.size() == 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("flushing single batch (size: %d)", batch.size());
        }
        AtomicReference<CompletableFuture<FlushBatchResult>> future = new AtomicReference<>();
        lock.executeInWriteLock(() -> {
            future.set(bulkUpdateService.submitBatch(batch)
                .whenComplete((flushBatchResult, throwable) -> {
                    AtomicBoolean oneOrMoreItemsFailed = new AtomicBoolean();
                    lock.executeInWriteLock(() -> {
                        pendingFutures.remove(future.get());
                        if (throwable != null) {
                            submittedItems.removeAll(batch);
                            todoItems.addAll(batch);
                        }
                        if (flushBatchResult != null) {
                            int i = 0;
                            for (BulkItemResponse bulkItemResponse : flushBatchResult.getBulkResponse().getItems()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                BulkItem bulkItem = batch.get(i++);
                                submittedItems.remove(bulkItem);
                                if (failure != null) {
                                    bulkItem.incrementFailCount();
                                    if (bulkItem.getFailCount() >= maxFailCount) {
                                        LOGGER.error("bulk item failed %d times: %s", bulkItem.getFailCount(), bulkItem);
                                        oneOrMoreItemsFailed.set(true);
                                        continue;
                                    }
                                    failures.add(new BulkItemFailure(bulkItem, bulkItemResponse));
                                }
                            }
                            pendingFutures.remove(future.get());
                        }
                    });

                    Set<String> indexNames = batch.stream().map(BulkItem::getIndexName).collect(Collectors.toSet());
                    for (String indexName : indexNames) {
                        indexRefreshTracker.pushChange(indexName);
                    }

                    if (oneOrMoreItemsFailed.get()) {
                        throw new VertexiumException("One or more items failed");
                    }
                }));
            pendingFutures.add(batch, future.get());
        });
        return future.get();
    }

    private boolean handleFailures(long beforeTime) {
        if (failures.size(beforeTime) == 0) {
            return false;
        }
        boolean hadFailures = false;
        synchronized (failures) {
            while (failures.size(beforeTime) > 0) {
                BulkItemFailure failure = failures.peek();
                if (failure == null) {
                    continue;
                }
                try {
                    BulkItem bulkItem = failure.getBulkItem();
                    BulkItemResponse bulkItemResponse = failure.getBulkItemResponse();
                    long nextRetryTime = (long) (bulkItem.getCreatedOrLastTriedTime() + (10 * Math.pow(2, bulkItem.getFailCount())));
                    long currentTime = System.currentTimeMillis();
                    long timeToWait = nextRetryTime - currentTime;
                    try {
                        if (timeToWait > 0) {
                            Thread.sleep(timeToWait);
                        }
                        AtomicBoolean retry = new AtomicBoolean();
                        bulkUpdateService.handleFailure(bulkItem, bulkItemResponse, retry);
                        hadFailures = true;
                        if (retry.get()) {
                            todoItems.add(bulkItem);
                        }
                    } catch (Exception ex) {
                        LOGGER.error("bulk item handleFailure failed: %s", bulkItem, ex);
                    }
                } finally {
                    failures.remove(failure);
                }
            }
        }
        return hadFailures;
    }

    private synchronized List<BulkItem> getBatch() {
        return lock.executeInWriteLock(() -> {
            int batchSizeInBytes = 0;
            List<BulkItem> results = new ArrayList<>();
            while (true) {
                BulkItem item = todoItems.peek();
                if (item == null) {
                    break;
                }
                batchSizeInBytes += item.getSize();
                if (results.size() > 0 && batchSizeInBytes > maxBatchSizeInBytes) {
                    break;
                }
                item = todoItems.dequeue();
                if (item == null) {
                    break;
                }
                submittedItems.add(item);
                results.add(item);
                if (results.size() >= maxBatchSize) {
                    break;
                }
            }
            return results;
        });
    }

    public void flush() {
        LOGGER.trace("flushing");
        long flushStartTime = System.currentTimeMillis();
        while (hasItemsToFlushOrWaitingForItemsToFlush(flushStartTime)) {
            if (failures.size(flushStartTime) > 0) {
                if (handleFailures(flushStartTime)) {
                    flushStartTime = System.currentTimeMillis();
                }
            } else if (todoItems.size(flushStartTime) > 0) {
                flushSingleBatch();
            } else if (pendingFutures.size(flushStartTime) > 0) {
                CompletableFuture<FlushBatchResult> pendingFuture = pendingFutures.peek();
                if (pendingFuture == null) {
                    continue;
                }
                try {
                    LOGGER.trace("waiting for pending future");
                    pendingFuture.get();
                } catch (Exception ex) {
                    throw new VertexiumException(ex);
                }
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new VertexiumException(e);
                }
            }
        }
    }

    private boolean hasItemsToFlushOrWaitingForItemsToFlush(long flushStartTime) {
        return lock.executeInReadLock(() -> {
            return todoItems.size(flushStartTime) > 0
                || submittedItems.size(flushStartTime) > 0
                || pendingFutures.size(flushStartTime) > 0
                || failures.size(flushStartTime) > 0;
        });
    }

    public void add(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> todoItems.add(bulkItem));
    }
}
