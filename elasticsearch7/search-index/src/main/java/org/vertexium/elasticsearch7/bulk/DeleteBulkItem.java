package org.vertexium.elasticsearch7.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.vertexium.ElementId;

public class DeleteBulkItem extends BulkItem {
    private final String docId;

    public DeleteBulkItem(
        ElementId elementId,
        String docId,
        DeleteRequest deleteRequest
    ) {
        super(deleteRequest.index(), elementId, deleteRequest);
        this.docId = docId;
    }

    public String getDocId() {
        return docId;
    }

    @Override
    public String toString() {
        return String.format("DeleteBulkItem {docId='%s'}", docId);
    }
}
