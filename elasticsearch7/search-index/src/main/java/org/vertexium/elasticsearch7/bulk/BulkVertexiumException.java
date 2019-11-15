package org.vertexium.elasticsearch7.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.vertexium.VertexiumException;

public class BulkVertexiumException extends VertexiumException {
    private final BulkItemResponse.Failure failure;

    public BulkVertexiumException(String message, BulkItemResponse.Failure failure) {
        super(message + ":" + failure.getMessage());
        this.failure = failure;
    }

    public BulkItemResponse.Failure getFailure() {
        return failure;
    }
}
