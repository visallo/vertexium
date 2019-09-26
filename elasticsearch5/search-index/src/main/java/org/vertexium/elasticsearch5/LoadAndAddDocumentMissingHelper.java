package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.elasticsearch5.bulk.BulkItem;
import org.vertexium.elasticsearch5.bulk.UpdateBulkItem;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collections;

public class LoadAndAddDocumentMissingHelper implements Elasticsearch5ExceptionHandler {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(LoadAndAddDocumentMissingHelper.class);

    public static void handleDocumentMissingException(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        BulkItem bulkItem,
        Authorizations authorizations
    ) {
        LOGGER.info("handleDocumentMissingException (bulkItem: %s)", bulkItem);
        if (bulkItem instanceof UpdateBulkItem) {
            UpdateBulkItem updateBulkItem = (UpdateBulkItem) bulkItem;
            if (updateBulkItem.getExtendedDataRowId() != null) {
                handleExtendedDataRow(graph, elasticsearch5SearchIndex, updateBulkItem, authorizations);
                return;
            }

            handleElement(graph, elasticsearch5SearchIndex, updateBulkItem, authorizations);
        } else {
            throw new VertexiumException("unhandled bulk item type: " + bulkItem.getClass().getName());
        }
    }

    protected static void handleElement(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        UpdateBulkItem bulkItem,
        Authorizations authorizations
    ) {
        Element element;
        switch (bulkItem.getElementId().getElementType()) {
            case VERTEX:
                element = graph.getVertex(bulkItem.getElementId().getId(), authorizations);
                break;
            case EDGE:
                element = graph.getEdge(bulkItem.getElementId().getId(), authorizations);
                break;
            default:
                throw new VertexiumException("Invalid element type: " + bulkItem.getElementId().getElementType());
        }
        elasticsearch5SearchIndex.addElement(
            graph,
            element,
            element.getAdditionalVisibilities(),
            null,
            authorizations
        );
    }

    protected static void handleExtendedDataRow(
        Graph graph,
        Elasticsearch5SearchIndex elasticsearch5SearchIndex,
        UpdateBulkItem bulkItem,
        Authorizations authorizations
    ) {
        ExtendedDataRowId id = new ExtendedDataRowId(
            bulkItem.getElementId().getElementType(),
            bulkItem.getElementId().getId(),
            bulkItem.getExtendedDataTableName(),
            bulkItem.getExtendedDataRowId()
        );
        ExtendedDataRow row = graph.getExtendedData(id, authorizations);
        elasticsearch5SearchIndex.addExtendedData(
            graph,
            bulkItem.getElementLocation(),
            Collections.singletonList(row),
            authorizations
        );
    }
}
