package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.vertexium.ElementId;
import org.vertexium.ElementLocation;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5Graph;
import org.vertexium.elasticsearch5.IndexService;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkUpdateService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateService.class);
    private final Elasticsearch5Graph graph;
    private final BulkUpdateQueue bulkUpdateQueue;
    private final Executor threadPool;
    private final Duration bulkRequestTimeout;

    public BulkUpdateService(
        Elasticsearch5Graph graph,
        IndexService indexService,
        BulkUpdateServiceConfiguration configuration
    ) {
        this.graph = graph;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(configuration.getQueueDepth());
        this.threadPool = new ThreadPoolExecutor(
            configuration.getCorePoolSize(),
            configuration.getMaximumPoolSize(),
            0,
            TimeUnit.MILLISECONDS,
            workQueue,
            r -> {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("vertexium-bulk-update-" + thread.getId());
                return thread;
            }
        );
        this.bulkRequestTimeout = configuration.getBulkRequestTimeout();
        BulkUpdateQueueConfiguration bulkUpdateQueueConfiguration = new BulkUpdateQueueConfiguration()
            .setMaxBatchSize(configuration.getMaxBatchSize())
            .setMaxBatchSizeInBytes(configuration.getMaxBatchSizeInBytes())
            .setMaxFailCount(configuration.getMaxFailCount());
        this.bulkUpdateQueue = new BulkUpdateQueue(indexService, this, bulkUpdateQueueConfiguration);
    }

    private boolean containsElementId(String elementId) {
        return bulkUpdateQueue.containsElementId(elementId);
    }

    private CompletableFuture<FlushBatchResult> flushSingleBatch() {
        return bulkUpdateQueue.flushSingleBatch();
    }

    public void flush() {
        bulkUpdateQueue.flush();
    }

    public void addUpdate(ElementLocation elementLocation, UpdateRequest updateRequest) {
        addUpdate(elementLocation, null, null, updateRequest);
    }

    public void addIndex(ElementLocation elementLocation, IndexRequest indexRequest) {
        bulkUpdateQueue.add(
            new IndexBulkItem(elementLocation, indexRequest)
        );
    }

    public void addUpdate(
        ElementLocation elementLocation,
        String extendedDataTableName,
        String rowId,
        UpdateRequest updateRequest
    ) {
        bulkUpdateQueue.add(
            new UpdateBulkItem(elementLocation, extendedDataTableName, rowId, updateRequest)
        );
    }

    public void addDelete(
        ElementId elementId,
        DeleteRequest deleteRequest
    ) {
        bulkUpdateQueue.add(new DeleteBulkItem(elementId, deleteRequest));
    }

    public void addDeleteByQuery(DeleteByQueryRequestBuilder request) {
        bulkUpdateQueue.addDeleteByQuery(request);
    }

    CompletableFuture<FlushBatchResult> submitDeleteByQuery(DeleteByQueryRequestBuilder request) {
        CompletableFuture<FlushBatchResult> future = new CompletableFuture<>();
        this.threadPool.execute(() -> {
            try {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("submitting delete by query");
                }
                BulkByScrollResponse response = request.get(new TimeValue(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS));
                if (response.getSearchFailures().size() > 0) {
                    throw new VertexiumException("Failed to run search for delete by query");
                }
                if (response.getBulkFailures().size() > 0) {
                    throw new VertexiumException("Failed to run bulk delete for delete by query");
                }
                future.complete(new FlushBatchResult(null));
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    CompletableFuture<FlushBatchResult> submitBatch(List<BulkItem> batch) {
        CompletableFuture<FlushBatchResult> future = new CompletableFuture<>();
        this.threadPool.execute(() -> {
            try {
                BulkRequest bulkRequest = flushObjectToBulkRequest(batch);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("submitting bulk request (size: %d)", bulkRequest.requests().size());
                }
                BulkResponse bulkResponse = graph.getClient()
                    .bulk(bulkRequest)
                    .get(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);
                future.complete(new FlushBatchResult(bulkResponse));
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    private BulkRequest flushObjectToBulkRequest(List<BulkItem> bulkItems) {
        BulkRequestBuilder builder = graph.getClient().prepareBulk();
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

    public void handleFailure(BulkItem bulkItem, BulkItemResponse item, AtomicBoolean retry) throws Exception {
        graph.handleBulkFailure(bulkItem, item, retry);
    }

    public void shutdown() {
        bulkUpdateQueue.flush();
    }

    public void flushUntilElementIdIsComplete(String elementId) {
        while (containsElementId(elementId)) {
            try {
                flushSingleBatch().get();
            } catch (Exception ex) {
                throw new VertexiumException(ex);
            }
        }
    }
}
