package org.vertexium.elasticsearch7.bulk;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.vertexium.VertexiumObjectId;

import java.util.ArrayList;
import java.util.List;

public abstract class BulkItem<T extends Item> {
    private final String indexName;
    private final String type;
    private final String documentId;
    private final VertexiumObjectId vertexiumObjectId;
    private final List<T> items = new ArrayList<>();

    public BulkItem(
        String indexName,
        String type,
        String documentId,
        VertexiumObjectId vertexiumObjectId
    ) {
        this.indexName = indexName;
        this.type = type;
        this.documentId = documentId;
        this.vertexiumObjectId = vertexiumObjectId;
    }

    public void add(T item) {
        items.add(item);
    }

    public abstract void addToBulkRequest(Client client, BulkRequestBuilder bulkRequestBuilder);

    public void complete() {
        for (T item : items) {
            item.complete();
        }
    }

    public void completeExceptionally(Exception exception) {
        for (T item : items) {
            item.completeExceptionally(exception);
        }
    }

    public void incrementFailCount() {
        for (T item : items) {
            item.incrementFailCount();
        }
    }

    public void updateLastTriedTime() {
        for (T item : items) {
            item.updateLastTriedTime();
        }
    }

    public String getDocumentId() {
        return documentId;
    }

    public int getFailCount() {
        return items.stream()
            .map(Item::getFailCount)
            .min(Integer::compareTo)
            .orElse(0);
    }

    public String getIndexName() {
        return indexName;
    }

    public List<T> getItems() {
        return items;
    }

    public abstract int getSize();

    public String getType() {
        return type;
    }

    public VertexiumObjectId getVertexiumObjectId() {
        return vertexiumObjectId;
    }
}
