package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;

import java.util.Collections;

public class LoadAndAddDocumentMissingHelper implements Elasticsearch5ExceptionHandler {
    public static void handleDocumentMissingException(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        FlushObjectQueue.FlushObject flushObject,
        Exception ex,
        Authorizations authorizations
    ) {
        graph.flush();

        if (flushObject.getExtendedDataRowId() != null) {
            handleExtendedDataRow(graph, elasticsearch5SearchIndex, flushObject, authorizations);
            return;
        }

        handleElement(graph, elasticsearch5SearchIndex, flushObject, authorizations);
    }

    protected static void handleElement(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        FlushObjectQueue.FlushObject flushObject,
        Authorizations authorizations
    ) {
        Element element;
        switch (flushObject.getElementLocation().getElementType()) {
            case VERTEX:
                element = graph.getVertex(flushObject.getElementLocation().getId(), authorizations);
                break;
            case EDGE:
                element = graph.getEdge(flushObject.getElementLocation().getId(), authorizations);
                break;
            default:
                throw new VertexiumException("Invalid element type: " + flushObject.getElementLocation().getElementType());
        }
        elasticsearch5SearchIndex.addElement(graph, element, authorizations);
    }

    protected static void handleExtendedDataRow(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        FlushObjectQueue.FlushObject flushObject,
        Authorizations authorizations
    ) {
        ExtendedDataRowId id = new ExtendedDataRowId(
            flushObject.getElementLocation().getElementType(),
            flushObject.getElementLocation().getId(),
            flushObject.getExtendedDataTableName(),
            flushObject.getExtendedDataRowId()
        );
        ExtendedDataRow row = graph.getExtendedData(id, authorizations);
        elasticsearch5SearchIndex.addExtendedData(
            graph,
            flushObject.getElementLocation(),
            Collections.singletonList(row),
            authorizations
        );
    }
}
