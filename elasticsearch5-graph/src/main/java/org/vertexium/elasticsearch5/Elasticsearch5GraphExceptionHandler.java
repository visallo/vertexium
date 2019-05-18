package org.vertexium.elasticsearch5;

public interface Elasticsearch5GraphExceptionHandler {
    void handleDocumentMissingException(Elasticsearch5Graph graph, FlushObjectQueue.FlushObject flushObject, Exception ex);
}
