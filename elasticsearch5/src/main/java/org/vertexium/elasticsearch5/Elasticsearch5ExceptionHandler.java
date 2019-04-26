package org.vertexium.elasticsearch5;

import org.vertexium.Graph;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;

public interface Elasticsearch5ExceptionHandler {
    default void handleDocumentMissingException(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        FlushObjectQueue.FlushObject flushObject,
        Exception ex
    ) throws Exception {
        throw ex;
    }
}
