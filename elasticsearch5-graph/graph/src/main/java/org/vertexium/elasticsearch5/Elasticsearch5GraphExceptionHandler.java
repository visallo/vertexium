package org.vertexium.elasticsearch5;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.elasticsearch5.bulk.BulkItem;

import java.util.concurrent.atomic.AtomicBoolean;

public interface Elasticsearch5GraphExceptionHandler {
    void handleBulkFailure(
        Elasticsearch5Graph graph,
        BulkItem bulkItem,
        BulkItemResponse bulkItemResponse,
        AtomicBoolean retry
    );
}
