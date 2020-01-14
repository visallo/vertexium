package org.vertexium.elasticsearch5.bulk;

import org.vertexium.VertexiumObjectId;

import java.util.concurrent.CompletableFuture;

public abstract class Item {
    private final String indexName;
    private final String type;
    private final String documentId;
    private final VertexiumObjectId vertexiumObjectId;
    private final CompletableFuture<Void> addedToBatchFuture;
    private final CompletableFuture<Void> completedFuture;
    private long createdOrLastTriedTime;
    private int failCount;

    public Item(String indexName, String type, String documentId, VertexiumObjectId vertexiumObjectId) {
        this.indexName = indexName;
        this.type = type;
        this.documentId = documentId;
        this.vertexiumObjectId = vertexiumObjectId;

        this.addedToBatchFuture = new CompletableFuture<>();
        this.completedFuture = new CompletableFuture<>();
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public void complete() {
        addedToBatchFuture.complete(null);
        completedFuture.complete(null);
    }

    public void completeExceptionally(Exception exception) {
        addedToBatchFuture.complete(null);
        completedFuture.completeExceptionally(exception);
    }

    public void incrementFailCount() {
        failCount++;
    }

    public void updateLastTriedTime() {
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public CompletableFuture<Void> getAddedToBatchFuture() {
        return addedToBatchFuture;
    }

    public CompletableFuture<Void> getCompletedFuture() {
        return completedFuture;
    }

    public long getCreatedOrLastTriedTime() {
        return createdOrLastTriedTime;
    }

    public String getDocumentId() {
        return documentId;
    }

    public int getFailCount() {
        return failCount;
    }

    public String getIndexName() {
        return indexName;
    }

    public abstract int getSize();

    public String getType() {
        return type;
    }

    public VertexiumObjectId getVertexiumObjectId() {
        return vertexiumObjectId;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {vertexiumObjectId=%s}@%s",
            getClass().getSimpleName(),
            vertexiumObjectId,
            Integer.toHexString(hashCode())
        );
    }
}
