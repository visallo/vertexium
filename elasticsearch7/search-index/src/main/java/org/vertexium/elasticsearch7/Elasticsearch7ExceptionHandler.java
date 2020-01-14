package org.vertexium.elasticsearch7;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.Graph;
import org.vertexium.elasticsearch7.bulk.BulkItem;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public interface Elasticsearch7ExceptionHandler {
    default void handleBulkFailure(
        Graph graph,
        Elasticsearch7SearchIndex elasticsearch7SearchIndex,
        BulkItem<?> bulkItem,
        BulkItemResponse bulkItemResponse,
        AtomicBoolean retry
    ) throws Exception {
        VertexiumLoggerFactory.getLogger(Elasticsearch7ExceptionHandler.class)
            .error("bulk failure: %s: %s", bulkItem, bulkItemResponse.getFailureMessage());
        retry.set(true);
    }
}
