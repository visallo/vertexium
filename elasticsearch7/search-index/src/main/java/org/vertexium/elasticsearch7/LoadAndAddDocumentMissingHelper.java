package org.vertexium.elasticsearch7;

import org.vertexium.*;
import org.vertexium.elasticsearch7.bulk.BulkItem;
import org.vertexium.elasticsearch7.bulk.UpdateBulkItem;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collections;

public class LoadAndAddDocumentMissingHelper implements Elasticsearch7ExceptionHandler {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(LoadAndAddDocumentMissingHelper.class);

    public static void handleDocumentMissingException(
        Graph graph,
        Elasticsearch7SearchIndex elasticsearch7SearchIndex,
        BulkItem bulkItem,
        Authorizations authorizations
    ) {
        LOGGER.info("handleDocumentMissingException (bulkItem: %s)", bulkItem);
        if (bulkItem instanceof UpdateBulkItem) {
            UpdateBulkItem updateBulkItem = (UpdateBulkItem) bulkItem;
            if (updateBulkItem.getExtendedDataRowId() != null) {
                handleExtendedDataRow(graph, elasticsearch7SearchIndex, updateBulkItem, authorizations);
                return;
            }

            handleElement(graph, elasticsearch7SearchIndex, updateBulkItem, authorizations);
        } else {
            throw new VertexiumException("unhandled bulk item type: " + bulkItem.getClass().getName());
        }
    }

    protected static void handleElement(
        Graph graph,
        Elasticsearch7SearchIndex elasticsearch7SearchIndex,
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
        if (element == null) {
            throw new VertexiumException("Could not find element: " + bulkItem.getElementId().getId());
        }
        elasticsearch7SearchIndex.addElement(
            graph,
            element,
            element.getAdditionalVisibilities(),
            null,
            false,
            authorizations
        );
    }

    protected static void handleExtendedDataRow(
        Graph graph,
        Elasticsearch7SearchIndex elasticsearch7SearchIndex,
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
        elasticsearch7SearchIndex.addExtendedData(
            graph,
            bulkItem.getElementLocation(),
            Collections.singletonList(row),
            authorizations
        );
    }
}
