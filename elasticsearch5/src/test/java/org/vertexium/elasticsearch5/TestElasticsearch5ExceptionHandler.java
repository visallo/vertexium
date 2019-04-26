package org.vertexium.elasticsearch5;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class TestElasticsearch5ExceptionHandler implements Elasticsearch5ExceptionHandler {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(LoadAndAddDocumentMissingHelper.class);
    public static Authorizations authorizations;

    @Override
    public void handleDocumentMissingException(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        FlushObjectQueue.FlushObject flushObject,
        Exception ex
    ) {
        LOGGER.warn("document missing %s, attempting to add document", flushObject, ex);
        LoadAndAddDocumentMissingHelper.handleDocumentMissingException(graph, elasticsearch5SearchIndex, flushObject, ex, authorizations);
    }
}
