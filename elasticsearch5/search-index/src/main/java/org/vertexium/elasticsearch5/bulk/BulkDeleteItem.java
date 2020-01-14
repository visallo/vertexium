package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.vertexium.VertexiumObjectId;

import static org.vertexium.elasticsearch5.bulk.BulkUtils.calculateSizeOfId;

public class BulkDeleteItem extends BulkItem<DeleteItem> {
    private int size;

    public BulkDeleteItem(
        String indexName,
        String type,
        String documentId,
        VertexiumObjectId vertexiumObjectId
    ) {
        super(indexName, type, documentId, vertexiumObjectId);
        size = getIndexName().length()
            + getType().length()
            + getDocumentId().length()
            + calculateSizeOfId(getVertexiumObjectId());
    }

    @Override
    public void addToBulkRequest(Client client, BulkRequestBuilder bulkRequestBuilder) {
        DeleteRequest deleteRequest = client
            .prepareDelete(getIndexName(), getType(), getDocumentId())
            .request();
        bulkRequestBuilder.add(deleteRequest);
    }

    @Override
    public int getSize() {
        return size;
    }
}
