package org.vertexium.elasticsearch7.utils;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.vertexium.VertexiumException;

public class ElasticsearchRequestUtils {
    public static int getSize(ActionRequest request) {
        if (request instanceof UpdateRequest) {
            return getSizeOfUpdateRequest((UpdateRequest) request);
        } else if (request instanceof DeleteRequest) {
            return getSizeOfDeleteRequest((DeleteRequest) request);
        } else {
            throw new VertexiumException("unhandled action request type: " + request.getClass().getName());
        }
    }

    private static int getSizeOfDeleteRequest(DeleteRequest request) {
        return request.id().length();
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
