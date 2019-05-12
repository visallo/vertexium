package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.vertexium.ElementId;

public class DeleteBulkItem extends BulkItem {
    public DeleteBulkItem(
        ElementId elementId,
        DeleteRequest deleteRequest
    ) {
        super(deleteRequest.index(), elementId, deleteRequest);
    }
}
