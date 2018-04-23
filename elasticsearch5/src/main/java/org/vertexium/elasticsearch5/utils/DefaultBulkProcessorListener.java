package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

public abstract class DefaultBulkProcessorListener implements BulkProcessor.Listener {
    @Override
    public void beforeBulk(long executionId, BulkRequest request) {

    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

    }
}
