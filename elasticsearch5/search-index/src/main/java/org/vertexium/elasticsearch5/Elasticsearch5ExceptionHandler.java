package org.vertexium.elasticsearch5;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.Graph;
import org.vertexium.elasticsearch5.bulk.BulkItem;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public interface Elasticsearch5ExceptionHandler {
    default void handleBulkFailure(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        BulkItem<?> bulkItem,
        BulkItemResponse bulkItemResponse,
        AtomicBoolean retry
    ) throws Exception {
        VertexiumLoggerFactory.getLogger(Elasticsearch5ExceptionHandler.class)
            .error("bulk failure: %s: %s", bulkItem, bulkItemResponse.getFailureMessage());
        retry.set(true);
    }
}
