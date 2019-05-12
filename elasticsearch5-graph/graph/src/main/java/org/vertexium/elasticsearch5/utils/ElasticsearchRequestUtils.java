package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.vertexium.VertexiumException;

public class ElasticsearchRequestUtils {
    public static int getSize(ActionRequest request) {
        if (request instanceof UpdateRequest) {
            return getSizeOfUpdateRequest((UpdateRequest) request);
        } else if (request instanceof DeleteRequest) {
            return getSizeOfDeleteRequest((DeleteRequest) request);
        } else if (request instanceof IndexRequest) {
            return getSizeOfIndexRequest((IndexRequest) request);
        } else if (request instanceof DeleteByQueryRequest) {
            return getSizeOfDeleteByQueryRequest((DeleteByQueryRequest) request);
        } else {
            throw new VertexiumException("unhandled action request type: " + request.getClass().getName());
        }
    }

    private static int getSizeOfDeleteByQueryRequest(DeleteByQueryRequest request) {
        return 500; // TODO not sure how to get this size
    }

    private static int getSizeOfDeleteRequest(DeleteRequest request) {
        return request.id().length();
    }

    private static int getSizeOfIndexRequest(IndexRequest request) {
        return request.source().length();
    }

    private static int getSizeOfUpdateRequest(UpdateRequest updateRequest) {
        int sizeInBytes = 0;
        if (updateRequest.doc() != null) {
            sizeInBytes += updateRequest.doc().source().length();
        }
        if (updateRequest.upsertRequest() != null) {
            sizeInBytes += updateRequest.upsertRequest().source().length();
        }
        if (updateRequest.script() != null) {
            sizeInBytes += updateRequest.script().getIdOrCode().length() * 2;
        }
        return sizeInBytes;
    }
}
