package org.vertexium.elasticsearch7;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.rest.RestStatus;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.elasticsearch7.bulk.BulkItem;
import org.vertexium.elasticsearch7.bulk.Item;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestElasticsearch7ExceptionHandler implements Elasticsearch7ExceptionHandler {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(LoadAndAddDocumentMissingHelper.class);
    public static Authorizations authorizations;
    private static AtomicInteger numberOfTimesCalled = new AtomicInteger();

    @Override
    public void handleBulkFailure(
        Graph graph,
        Elasticsearch7SearchIndex elasticsearch7SearchIndex,
        BulkItem<?> bulkItem,
        BulkItemResponse bulkItemResponse,
        AtomicBoolean retry
    ) {
        numberOfTimesCalled.incrementAndGet();
        LOGGER.warn("bulk failure on item %s: %s", bulkItem, bulkItemResponse == null ? null : bulkItemResponse.getFailure());
        if (bulkItemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
            LoadAndAddDocumentMissingHelper.handleDocumentMissingException(graph, elasticsearch7SearchIndex, bulkItem, authorizations);
        } else {
            retry.set(true);
        }
    }

    public static void clearNumberOfTimesCalled() {
        numberOfTimesCalled.set(0);
    }

    public static int getNumberOfTimesCalled() {
        return numberOfTimesCalled.get();
    }
}
