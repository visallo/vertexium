package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.bulk.BulkResponse;

public class FlushBatchResult {
    private BulkResponse bulkResponse;

    public FlushBatchResult(BulkResponse bulkResponse) {
        this.bulkResponse = bulkResponse;
    }

    public BulkResponse getBulkResponse() {
        return bulkResponse;
    }
}
