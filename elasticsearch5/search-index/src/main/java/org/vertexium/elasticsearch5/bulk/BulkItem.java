package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.ActionRequest;
import org.vertexium.ElementId;
import org.vertexium.elasticsearch5.utils.ElasticsearchRequestUtils;

public class BulkItem {
    private final String indexName;
    private final ElementId elementId;
    private final int size;
    private final ActionRequest actionRequest;
    private final long createdTime;
    private final BulkItemCompletableFuture addedToBatchFuture;
    private final BulkItemCompletableFuture completedFuture;
    private final StackTraceElement[] stackTrace;
    private long createdOrLastTriedTime;
    private int failCount;

    public BulkItem(
        String indexName,
        ElementId elementId,
        ActionRequest actionRequest
    ) {
        this.addedToBatchFuture = new BulkItemCompletableFuture(this);
        this.completedFuture = new BulkItemCompletableFuture(this);
        this.indexName = indexName;
        this.elementId = elementId;
        this.size = ElasticsearchRequestUtils.getSize(actionRequest);
        this.actionRequest = actionRequest;
        this.createdOrLastTriedTime = this.createdTime = System.currentTimeMillis();
        if (BulkUpdateService.LOGGER_STACK_TRACE.isTraceEnabled()) {
            this.stackTrace = Thread.currentThread().getStackTrace();
        } else {
            this.stackTrace = null;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public ElementId getElementId() {
        return elementId;
    }

    public int getSize() {
        return size;
    }

    public ActionRequest getActionRequest() {
        return actionRequest;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getCreatedOrLastTriedTime() {
        return createdOrLastTriedTime;
    }

    public void updateLastTriedTime() {
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public void incrementFailCount() {
        failCount++;
    }

    public int getFailCount() {
        return failCount;
    }

    public BulkItemCompletableFuture getAddedToBatchFuture() {
        return addedToBatchFuture;
    }

    public BulkItemCompletableFuture getCompletedFuture() {
        return completedFuture;
    }

    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    @Override
    public String toString() {
        return String.format("%s {elementId=%s, actionRequest=%s}", getClass().getSimpleName(), elementId, actionRequest);
    }
}
