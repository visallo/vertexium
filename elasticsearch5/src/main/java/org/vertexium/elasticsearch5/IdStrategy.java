package org.vertexium.elasticsearch5;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.ElementLocation;
import org.vertexium.ElementType;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.utils.Ascii85;
import org.vertexium.elasticsearch5.utils.Murmur3;

public class IdStrategy {
    public static final String ELEMENT_TYPE = "e";
    private static final String EXTENDED_DATA_FIELD_SEPARATOR = ":";

    public String getType() {
        return ELEMENT_TYPE;
    }

    public String createExtendedDataDocId(ElementLocation elementLocation, String tableName, String rowId) {
        return createExtendedDataDocId(elementLocation.getId(), tableName, rowId);
    }

    public String createExtendedDataDocId(ExtendedDataRowId rowId) {
        return createExtendedDataDocId(rowId.getElementId(), rowId.getTableName(), rowId.getRowId());
    }

    public String createExtendedDataDocId(String elementId, String tableName, String rowId) {
        return createDocId(elementId + EXTENDED_DATA_FIELD_SEPARATOR + tableName + EXTENDED_DATA_FIELD_SEPARATOR + rowId);
    }

    public String createElementDocId(ElementLocation elementLocation) {
        return createDocId(elementLocation.getId());
    }

    private String createDocId(String s) {
        byte[] hash = Murmur3.hash128(s.getBytes());
        return Ascii85.encode(hash);
    }

    public ExtendedDataRowId extendedDataRowIdFromSearchHit(SearchHit hit) {
        SearchHitField elementTypeField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        if (elementTypeField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        }
        ElementType elementType = ElasticsearchDocumentType.parse(elementTypeField.getValue().toString()).toElementType();

        SearchHitField elementIdField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        if (elementIdField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        }
        String elementId = elementIdField.getValue();

        SearchHitField tableNameField = hit.getFields().get(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME);
        if (tableNameField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME);
        }
        String tableName = tableNameField.getValue();

        SearchHitField rowIdField = hit.getFields().get(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME);
        if (rowIdField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME);
        }
        String rowId = rowIdField.getValue();

        return new ExtendedDataRowId(elementType, elementId, tableName, rowId);
    }

    public String vertexIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    public String edgeIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    private String elementIdFromSearchHit(SearchHit hit) {
        SearchHitField elementIdField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        if (elementIdField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        }
        return elementIdField.getValue();
    }

    public Object fromSearchHit(SearchHit hit) {
        ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
        if (dt == null) {
            return null;
        }
        switch (dt) {
            case EDGE:
                return edgeIdFromSearchHit(hit);
            case VERTEX:
                return vertexIdFromSearchHit(hit);
            case EDGE_EXTENDED_DATA:
            case VERTEX_EXTENDED_DATA:
                return extendedDataRowIdFromSearchHit(hit);
            default:
                throw new VertexiumException("Unhandled document type: " + dt);
        }
    }
}
