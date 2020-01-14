package org.vertexium.elasticsearch7.bulk;

import org.vertexium.VertexiumObjectId;

import static org.vertexium.elasticsearch7.bulk.BulkUtils.calculateSizeOfId;

public class DeleteItem extends Item {
    private final int size;

    public DeleteItem(String indexName, String type, String docId, VertexiumObjectId vertexiumObjectId) {
        super(indexName, type, docId, vertexiumObjectId);
        size = getIndexName().length()
            + getType().length()
            + getDocumentId().length()
            + calculateSizeOfId(vertexiumObjectId);
    }

    @Override
    public int getSize() {
        return size;
    }
}
