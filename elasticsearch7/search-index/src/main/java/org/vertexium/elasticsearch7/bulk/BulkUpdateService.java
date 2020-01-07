package org.vertexium.elasticsearch7.bulk;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.vertexium.ElementId;
import org.vertexium.ElementLocation;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch7.Elasticsearch7SearchIndex;
import org.vertexium.elasticsearch7.IndexRefreshTracker;
import org.vertexium.metric.Histogram;
import org.vertexium.metric.Timer;
import org.vertexium.metric.VertexiumMetricRegistry;
import org.vertexium.util.LimitedLinkedBlockingQueue;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class BulkUpdateService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateService.class);
    private static final String LOGGER_STACK_TRACE_NAME = BulkUpdateService.class.getName() + ".STACK_TRACE";
    static final VertexiumLogger LOGGER_STACK_TRACE = VertexiumLoggerFactory.getLogger(LOGGER_STACK_TRACE_NAME);
    private final Elasticsearch7SearchIndex searchIndex;
    private final IndexRefreshTracker indexRefreshTracker;
    private final LimitedLinkedBlockingQueue<BulkItem> incomingItems = new LimitedLinkedBlockingQueue<>();
    private final OutstandingItemsList outstandingItems = new OutstandingItemsList();
    private final Thread processItemsThread;
    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;
    private final Timer flushUntilElementIdIsCompleteTimer;
    private final Timer processBatchTimer;
    private final Duration bulkRequestTimeout;
    private final ThreadPoolExecutor ioExecutor;
    private final int maxFailCount;
    private final BulkItemBatch batch;
    private volatile boolean shutdown;

    public BulkUpdateService(
        Elasticsearch7SearchIndex searchIndex,
        IndexRefreshTracker indexRefreshTracker,
        BulkUpdateServiceConfiguration configuration
    ) {
        this.searchIndex = searchIndex;
        this.indexRefreshTracker = indexRefreshTracker;

        this.ioExecutor = new ThreadPoolExecutor(
            configuration.getPoolSize(),
            configuration.getPoolSize(),
            10,
            TimeUnit.SECONDS,
            new LimitedLinkedBlockingQueue<>(configuration.getBacklogSize()),
            r -> {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("vertexium-es-processItems-io-" + thread.getId());
                return thread;
            }
        );

        this.processItemsThread = new Thread(this::processIncomingItemsIntoBatches);
        this.processItemsThread.setName("vertexium-es-processItems");
        this.processItemsThread.setDaemon(true);
        this.processItemsThread.start();

        this.bulkRequestTimeout = configuration.getBulkRequestTimeout();
        this.maxFailCount = configuration.getMaxFailCount();
        this.batch = new BulkItemBatch(
            configuration.getMaxBatchSize(),
            configuration.getMaxBatchSizeInBytes(),
            configuration.getBatchWindowTime()
        );

        VertexiumMetricRegistry metricRegistry = searchIndex.getMetricsRegistry();
        this.flushTimer = metricRegistry.getTimer(BulkUpdateService.class, "flush", "timer");
        this.processBatchTimer = metricRegistry.getTimer(BulkUpdateService.class, "processBatch", "timer");
        this.batchSizeHistogram = metricRegistry.getHistogram(BulkUpdateService.class, "batch", "histogram");
        this.flushUntilElementIdIsCompleteTimer = metricRegistry.getTimer(BulkUpdateService.class, "flushUntilElementIdIsComplete", "timer");
        metricRegistry.getGauge(metricRegistry.createName(BulkUpdateService.class, "outstandingItems", "size"), outstandingItems::size);
    }

    private void processIncomingItemsIntoBatches() {
        while (true) {
            try {
                if (shutdown) {
                    return;
                }

                BulkItem item = incomingItems.poll(100, TimeUnit.MILLISECONDS);
                if (batch.shouldFlushByTime()) {
                    flushBatch();
                }
                if (item == null) {
                    continue;
                }
                if (filterByRetryTime(item)) {
                    while (!batch.add(item)) {
                        flushBatch();
                    }
                    item.getAddedToBatchFuture().complete(null);
                }
            } catch (InterruptedException ex) {
                // we are shutting down so return
                return;
            } catch (Exception ex) {
                LOGGER.error("process items failed", ex);
            }
        }
    }

    private void flushBatch() {
        List<BulkItem> batchItems = batch.getItemsAndClear();
        if (batchItems.size() > 0) {
            ioExecutor.execute(() -> processBatch(batchItems));
        }
    }

    private boolean filterByRetryTime(BulkItem bulkItem) {
        if (bulkItem.getFailCount() == 0) {
            return true;
        }
        long nextRetryTime = (long) (bulkItem.getCreatedOrLastTriedTime() + (10 * Math.pow(2, bulkItem.getFailCount())));
        long currentTime = System.currentTimeMillis();
        if (nextRetryTime > currentTime) {
            // add it back into incomingItems, it will already be in outstandingItems
            incomingItems.add(bulkItem);
            return false;
        }
        return true;
    }

    private void processBatch(List<BulkItem> bulkItems) {
        processBatchTimer.time(() -> {
            try {
                batchSizeHistogram.update(bulkItems.size());

                BulkRequest bulkRequest = bulkItemsToBulkRequest(bulkItems);
                BulkResponse bulkResponse = searchIndex.getClient()
                    .bulk(bulkRequest)
                    .get(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);

                Set<String> indexNames = bulkItems.stream()
                    .peek(BulkItem::updateLastTriedTime)
                    .map(BulkItem::getIndexName)
                    .collect(Collectors.toSet());
                indexRefreshTracker.pushChanges(indexNames);

                int itemIndex = 0;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    BulkItem bulkItem = bulkItems.get(itemIndex++);
                    if (bulkItemResponse.isFailed()) {
                        handleFailure(bulkItem, bulkItemResponse);
                    } else {
                        handleSuccess(bulkItem);
                    }
                }
            } catch (Exception ex) {
                LOGGER.error("bulk request failed", ex);
                // if bulk failed try each item individually
                if (bulkItems.size() > 1) {
                    for (BulkItem bulkItem : bulkItems) {
                        processBatch(Lists.newArrayList(bulkItem));
                    }
                } else {
                    complete(bulkItems.get(0), ex);
                }
            }
        });
    }

    private void handleSuccess(BulkItem bulkItem) {
        complete(bulkItem, null);
    }

    private void handleFailure(BulkItem bulkItem, BulkItemResponse bulkItemResponse) {
        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
        bulkItem.incrementFailCount();
        if (bulkItem.getFailCount() >= maxFailCount) {
            complete(bulkItem, new BulkVertexiumException("fail count exceeded the max number of failures", failure));
        } else {
            AtomicBoolean retry = new AtomicBoolean(false);
            try {
                searchIndex.handleBulkFailure(bulkItem, bulkItemResponse, retry);
            } catch (Exception ex) {
                complete(bulkItem, ex);
                return;
            }
            if (retry.get()) {
                incomingItems.add(bulkItem);
            } else {
                complete(bulkItem, null);
            }
        }
    }

    private void complete(BulkItem bulkItem, Exception exception) {
        outstandingItems.remove(bulkItem);
        if (exception == null) {
            bulkItem.getCompletedFuture().complete(null);
        } else {
            bulkItem.getCompletedFuture().completeExceptionally(exception);
        }
    }

    private BulkRequest bulkItemsToBulkRequest(List<BulkItem> bulkItems) {
        BulkRequestBuilder builder = searchIndex.getClient().prepareBulk();
        for (BulkItem bulkItem : bulkItems) {
            ActionRequest actionRequest = bulkItem.getActionRequest();
            if (actionRequest instanceof IndexRequest) {
                builder.add((IndexRequest) actionRequest);
            } else if (actionRequest instanceof UpdateRequest) {
                builder.add((UpdateRequest) actionRequest);
            } else if (actionRequest instanceof DeleteRequest) {
                builder.add((DeleteRequest) actionRequest);
            } else {
                throw new VertexiumException("unhandled request type: " + actionRequest.getClass().getName());
            }
        }
        return builder.request();
    }

    public void flush() {
        flushTimer.time(() -> {
            try {
                List<BulkItem> items = outstandingItems.getItems();

                // wait for the items to be added to batches
                CompletableFuture.allOf(
                    items.stream()
                        .map(BulkItem::getAddedToBatchFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();

                // flush the current batch
                flushBatch();

                // wait for the items to complete
                CompletableFuture.allOf(
                    items.stream()
                        .map(BulkItem::getCompletedFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();
            } catch (Exception ex) {
                throw new VertexiumException("failed to flush", ex);
            }
        });
    }

    public CompletableFuture<Void> addUpdate(ElementLocation elementLocation, UpdateRequest updateRequest) {
        return add(new UpdateBulkItem(elementLocation, updateRequest));
    }

    public CompletableFuture<Void> addUpdate(
        ElementLocation elementLocation,
        String extendedDataTableName,
        String rowId,
        UpdateRequest updateRequest
    ) {
        return add(new UpdateBulkItem(elementLocation, extendedDataTableName, rowId, updateRequest));
    }

    public CompletableFuture<Void> addDelete(
        ElementId elementId,
        String docId,
        DeleteRequest deleteRequest
    ) {
        return add(new DeleteBulkItem(elementId, docId, deleteRequest));
    }

    private CompletableFuture<Void> add(BulkItem bulkItem) {
        outstandingItems.add(bulkItem);
        incomingItems.add(bulkItem);
        return bulkItem.getCompletedFuture();
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            this.processItemsThread.join(10_000);
        } catch (InterruptedException e) {
            // OK
        }

        ioExecutor.shutdown();
    }

    public void flushUntilElementIdIsComplete(String elementId) {
        long startTime = System.currentTimeMillis();
        BulkItemCompletableFuture lastFuture = null;
        while (true) {
            BulkItem item = outstandingItems.getItemForElementId(elementId);
            if (item == null) {
                break;
            }
            lastFuture = item.getCompletedFuture();
            try {
                if (!item.getCompletedFuture().isDone()) {
                    item.getAddedToBatchFuture().get();
                    flushBatch();
                }
                item.getCompletedFuture().get();
            } catch (Exception ex) {
                throw new VertexiumException("Failed to flushUntilElementIdIsComplete: " + elementId, ex);
            }
        }
        long endTime = System.currentTimeMillis();
        long delta = endTime - startTime;
        flushUntilElementIdIsCompleteTimer.update(delta, TimeUnit.MILLISECONDS);
        if (delta > 1_000) {
            String message = String.format(
                "flush of %s got stuck for %dms%s",
                elementId,
                delta,
                LOGGER_STACK_TRACE.isTraceEnabled()
                    ? ""
                    : String.format(" (for more information enable trace level on \"%s\")", LOGGER_STACK_TRACE_NAME)
            );
            if (delta > 60_000) {
                LOGGER.error("%s", message);
            } else if (delta > 10_000) {
                LOGGER.warn("%s", message);
            } else {
                LOGGER.info("%s", message);
            }
            if (LOGGER_STACK_TRACE.isTraceEnabled()) {
                logStackTrace("Current stack trace", Thread.currentThread().getStackTrace());
                if (lastFuture != null) {
                    StackTraceElement[] stackTrace = lastFuture.getBulkItem().getStackTrace();
                    if (stackTrace != null) {
                        logStackTrace("Other stack trace causing the delay", stackTrace);
                    }
                }
            }
        }
    }

    private void logStackTrace(String message, StackTraceElement[] stackTrace) {
        if (!LOGGER_STACK_TRACE.isTraceEnabled()) {
            return;
        }
        LOGGER_STACK_TRACE.trace(
            "%s",
            message + "\n" +
                Arrays.stream(stackTrace)
                    .map(e -> "   " + e.toString())
                    .collect(Collectors.joining("\n"))
        );
    }
}
