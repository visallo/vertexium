package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;

public class BulkItemFailure {
    private final BulkItem bulkItem;
    private final BulkItemResponse bulkItemResponse;

    public BulkItemFailure(BulkItem bulkItem, BulkItemResponse bulkItemResponse) {
        this.bulkItem = bulkItem;
        this.bulkItemResponse = bulkItemResponse;
    }

    public BulkItem getBulkItem() {
        return bulkItem;
    }

    public BulkItemResponse getBulkItemResponse() {
        return bulkItemResponse;
    }
}
