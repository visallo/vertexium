package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.IndexRefreshTracker;
import org.vertexium.metric.Counter;
import org.vertexium.metric.Timer;
import org.vertexium.metric.VertexiumMetricRegistry;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;
import org.vertexium.util.VertexiumReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BulkUpdateQueue {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateQueue.class);
    private final IndexRefreshTracker indexRefreshTracker;
    private final BulkUpdateService bulkUpdateService;
    private final BulkItemList todoItems = new BulkItemList();
    private final BulkItemList submittedItems = new BulkItemList();
    private final ConcurrentLinkedQueue<CompletableFuture<FlushBatchResult>> pendingFutures = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<BulkItemFailure> failures = new ConcurrentLinkedQueue<>();
    private final VertexiumReentrantReadWriteLock lock = new VertexiumReentrantReadWriteLock();
    private final int maxBatchSize;
    private final int maxBatchSizeInBytes;
    private final int maxFailCount;
    private final Timer flushTimer;
    private final Timer flushSingleBatchTimer;
    private final Counter failureCounter;

    public BulkUpdateQueue(
        IndexRefreshTracker indexRefreshTracker,
        BulkUpdateService bulkUpdateService,
        BulkUpdateQueueConfiguration configuration,
        VertexiumMetricRegistry metricRegistry
    ) {
        this.indexRefreshTracker = indexRefreshTracker;
        this.bulkUpdateService = bulkUpdateService;
        this.maxBatchSize = configuration.getMaxBatchSize();
        this.maxBatchSizeInBytes = configuration.getMaxBatchSizeInBytes();
        this.maxFailCount = configuration.getMaxFailCount();
        this.flushTimer = metricRegistry.getTimer(BulkUpdateQueue.class, "flush", "timer");
        this.flushSingleBatchTimer = metricRegistry.getTimer(BulkUpdateQueue.class, "flushSingleBatch", "timer");
        this.failureCounter = metricRegistry.getCounter(BulkUpdateQueue.class, "failure", "counter");
        metricRegistry.registerGauage(metricRegistry.createName(BulkUpdateQueue.class, "todo", "size"), todoItems::size);
        metricRegistry.registerGauage(metricRegistry.createName(BulkUpdateQueue.class, "pendingFutures", "size"), pendingFutures::size);
        metricRegistry.registerGauage(metricRegistry.createName(BulkUpdateQueue.class, "failures", "size"), failures::size);
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
        return this.flushSingleBatchTimer.time(() -> {
            AtomicReference<CompletableFuture<FlushBatchResult>> future = new AtomicReference<>();
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
                                    failureCounter.increment();
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
            pendingFutures.add(future.get());
            return future.get();
        });
    }

    private void handleFailures() {
        if (failures.size() == 0) {
            return;
        }
        synchronized (failures) {
            while (failures.size() > 0) {
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
        flushTimer.time(() -> {
            while (hasItemsToFlushOrWaitingForItemsToFlush()) {
                if (failures.size() > 0) {
                    handleFailures();
                } else if (todoItems.size() > 0) {
                    flushSingleBatch();
                } else if (pendingFutures.size() > 0) {
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
        });
    }

    private boolean hasItemsToFlushOrWaitingForItemsToFlush() {
        return lock.executeInReadLock(() -> {
            return todoItems.size() > 0
                || submittedItems.size() > 0
                || pendingFutures.size() > 0
                || failures.size() > 0;
        });
    }

    public void add(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> {
            todoItems.add(bulkItem);
        });
    }

    private static class BulkItemFailure {
        private final BulkItem bulkItem;
        private final BulkItemResponse bulkItemResponse;

        public BulkItemFailure(BulkItem bulkItem, BulkItemResponse bulkItemResponse) {
            this.bulkItem = bulkItem;
            this.bulkItemResponse = bulkItemResponse;
        }

        public BulkItem getBulkItem() {
            return bulkItem;
        }

        public BulkItemResponse getBulkItemResponse() {
            return bulkItemResponse;
        }
    }
}
