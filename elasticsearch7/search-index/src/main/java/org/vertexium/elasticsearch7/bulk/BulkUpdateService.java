package org.vertexium.elasticsearch7.bulk;

import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.vertexium.ElementId;
import org.vertexium.ElementLocation;
import org.vertexium.ExtendedDataRowId;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * All updates to Elasticsearch are sent using bulk requests to speed up indexing.
 * <p>
 * Duplicate element updates are collapsed into single updates to reduce the number of refreshes Elasticsearch
 * has to perform. See
 * - https://github.com/elastic/elasticsearch/issues/23792#issuecomment-296149685
 * - https://github.com/debadair/elasticsearch/commit/54cdf40bc5fdecce180ba2e242abca59c7bd1f11
 */
public class BulkUpdateService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateService.class);
    private static final String LOGGER_STACK_TRACE_NAME = BulkUpdateService.class.getName() + ".STACK_TRACE";
    static final VertexiumLogger LOGGER_STACK_TRACE = VertexiumLoggerFactory.getLogger(LOGGER_STACK_TRACE_NAME);
    private final Elasticsearch7SearchIndex searchIndex;
    private final IndexRefreshTracker indexRefreshTracker;
    private final LimitedLinkedBlockingQueue<Item> incomingItems = new LimitedLinkedBlockingQueue<>();
    private final OutstandingItemsList outstandingItems = new OutstandingItemsList();
    private final Thread processItemsThread;
    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;
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
            configuration.getBatchWindowTime(),
            configuration.getLogRequestSizeLimit()
        );

        VertexiumMetricRegistry metricRegistry = searchIndex.getMetricsRegistry();
        this.flushTimer = metricRegistry.getTimer(BulkUpdateService.class, "flush", "timer");
        this.processBatchTimer = metricRegistry.getTimer(BulkUpdateService.class, "processBatch", "timer");
        this.batchSizeHistogram = metricRegistry.getHistogram(BulkUpdateService.class, "batch", "histogram");
        metricRegistry.getGauge(metricRegistry.createName(BulkUpdateService.class, "outstandingItems", "size"), outstandingItems::size);
    }

    public CompletableFuture<Void> addDelete(
        String indexName,
        String type,
        String docId,
        ElementId elementId
    ) {
        return add(new DeleteItem(indexName, type, docId, elementId));
    }

    public CompletableFuture<Void> addElementUpdate(
        String indexName,
        String type,
        String docId,
        ElementLocation elementLocation,
        Map<String, String> source,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Collection<String> additionalVisibilities,
        Collection<String> additionalVisibilitiesToDelete,
        boolean existingElement
    ) {
        return add(new UpdateItem(
            indexName,
            type,
            docId,
            elementLocation,
            elementLocation,
            source,
            fieldsToSet,
            fieldsToRemove,
            fieldsToRename,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            existingElement
        ));
    }

    private CompletableFuture<Void> add(Item bulkItem) {
        outstandingItems.add(bulkItem);
        incomingItems.add(bulkItem);
        return bulkItem.getCompletedFuture();
    }

    public CompletableFuture<Void> addExtendedDataUpdate(
        String indexName,
        String type,
        String docId,
        ExtendedDataRowId extendedDataRowId,
        ElementLocation sourceElementLocation,
        Map<String, String> source,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Collection<String> additionalVisibilities,
        Collection<String> additionalVisibilitiesToDelete,
        boolean existingElement
    ) {
        return add(new UpdateItem(
            indexName,
            type,
            docId,
            extendedDataRowId,
            sourceElementLocation,
            source,
            fieldsToSet,
            fieldsToRemove,
            fieldsToRename,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            existingElement
        ));
    }

    private void complete(BulkItem<?> bulkItem, Exception exception) {
        outstandingItems.removeAll(bulkItem.getItems());
        if (exception == null) {
            bulkItem.complete();
        } else {
            bulkItem.completeExceptionally(exception);
        }
    }

    private boolean filterByRetryTime(Item bulkItem) {
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

    public void flush() {
        flushTimer.time(() -> {
            try {
                List<Item> items = outstandingItems.getCopyOfItems();

                // wait for the items to be added to batches
                CompletableFuture.allOf(
                    items.stream()
                        .map(Item::getAddedToBatchFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();

                // flush the current batch
                flushBatch();

                // wait for the items to complete
                CompletableFuture.allOf(
                    items.stream()
                        .map(Item::getCompletedFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();
            } catch (Exception ex) {
                throw new VertexiumException("failed to flush", ex);
            }
        });
    }

    private void flushBatch() {
        List<BulkItem<?>> batchItems = batch.getItemsAndClear();
        if (batchItems.size() > 0) {
            ioExecutor.execute(() -> processBatch(batchItems));
        }
    }

    private void handleFailure(BulkItem<?> bulkItem, BulkItemResponse bulkItemResponse) {
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
                incomingItems.addAll(bulkItem.getItems());
            } else {
                complete(bulkItem, null);
            }
        }
    }

    private void handleSuccess(BulkItem<?> bulkItem) {
        complete(bulkItem, null);
    }

    private void processBatch(List<BulkItem<?>> bulkItems) {
        processBatchTimer.time(() -> {
            try {
                batchSizeHistogram.update(bulkItems.size());

                BulkRequestBuilder bulkRequestBuilder = searchIndex.getClient().prepareBulk();
                for (BulkItem<?> bulkItem : bulkItems) {
                    bulkItem.addToBulkRequest(searchIndex.getClient(), bulkRequestBuilder);
                }

                outstandingItems.waitForItemToNotBeInflightAndMarkThemAsInflight(bulkItems);
                BulkResponse bulkResponse;
                try {
                    bulkResponse = searchIndex.getClient()
                        .bulk(bulkRequestBuilder.request())
                        .get(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } finally {
                    outstandingItems.markItemsAsNotInflight(bulkItems);
                }

                Set<String> indexNames = bulkItems.stream()
                    .peek(BulkItem::updateLastTriedTime)
                    .map(BulkItem::getIndexName)
                    .collect(Collectors.toSet());
                indexRefreshTracker.pushChanges(indexNames);

                int itemIndex = 0;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    BulkItem<?> bulkItem = bulkItems.get(itemIndex++);
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
                    for (BulkItem<?> bulkItem : bulkItems) {
                        processBatch(Lists.newArrayList(bulkItem));
                    }
                } else {
                    complete(bulkItems.get(0), ex);
                }
            }
        });
    }

    private void processIncomingItemsIntoBatches() {
        while (true) {
            try {
                if (shutdown) {
                    return;
                }

                Item item = incomingItems.poll(100, TimeUnit.MILLISECONDS);
                if (batch.shouldFlushByTime()) {
                    flushBatch();
                }
                if (item == null) {
                    continue;
                }
                try {
                    if (filterByRetryTime(item)) {
                        while (!batch.add(item)) {
                            flushBatch();
                        }
                        item.getAddedToBatchFuture().complete(null);
                    }
                } catch (Exception ex) {
                    LOGGER.error("process item (%s) failed", item, ex);
                    outstandingItems.remove(item);
                    item.completeExceptionally(new VertexiumException("Failed to process item", ex));
                }
            } catch (InterruptedException ex) {
                // we are shutting down so return
                return;
            } catch (Exception ex) {
                LOGGER.error("process items failed", ex);
            }
        }
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
}
