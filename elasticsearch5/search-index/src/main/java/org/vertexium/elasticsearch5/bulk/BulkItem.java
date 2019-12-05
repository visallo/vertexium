package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.ActionRequest;
import org.vertexium.ElementId;
import org.vertexium.elasticsearch5.utils.ElasticsearchRequestUtils;

import java.util.concurrent.CompletableFuture;

public class BulkItem {
    private final String indexName;
    private final ElementId elementId;
    private final int size;
    private final ActionRequest actionRequest;
    private final long createdTime;
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private long createdOrLastTriedTime;
    private int failCount;

    public BulkItem(
        String indexName,
        ElementId elementId,
        ActionRequest actionRequest
    ) {
        this.indexName = indexName;
        this.elementId = elementId;
        this.size = ElasticsearchRequestUtils.getSize(actionRequest);
        this.actionRequest = actionRequest;
        this.createdOrLastTriedTime = this.createdTime = System.currentTimeMillis();
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

    public CompletableFuture<Void> getFuture() {
        return future;
    }

    @Override
    public String toString() {
        return String.format("%s {elementId=%s, actionRequest=%s}", getClass().getSimpleName(), elementId, actionRequest);
    }
}
