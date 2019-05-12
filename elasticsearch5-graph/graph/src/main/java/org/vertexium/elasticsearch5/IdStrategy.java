package org.vertexium.elasticsearch5;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.ElementLocation;
import org.vertexium.ElementType;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.models.Mutation;
import org.vertexium.elasticsearch5.utils.Ascii85;
import org.vertexium.elasticsearch5.utils.Murmur3;

public class IdStrategy {
    private static final String ELEMENT_TYPE = "e";
    private static final String METADATA_TYPE = "m";
    private static final String EXTENDED_DATA_FIELD_SEPARATOR = ":";
    private static final String MUTATION_TYPE = "mut";

    public String getType() {
        return ELEMENT_TYPE;
    }

    public String getMetadataType() {
        return METADATA_TYPE;
    }

    public String getMutationType() {
        return MUTATION_TYPE;
    }

    public String createElementDocId(ElementLocation elementLocation) {
        return createDocId(elementLocation.getId());
    }

    public String createExtendedDataDocId(ElementLocation elementLocation, String tableName, String rowId) {
        return createExtendedDataDocId(elementLocation.getElementType(), elementLocation.getId(), tableName, rowId);
    }

    public String createExtendedDataDocId(ExtendedDataRowId rowId) {
        return createExtendedDataDocId(rowId.getElementType(), rowId.getElementId(), rowId.getTableName(), rowId.getRowId());
    }

    public String createExtendedDataDocId(ElementType elementType, String elementId, String tableName, String rowId) {
        return createDocId(
            elementType.name()
                + EXTENDED_DATA_FIELD_SEPARATOR
                + elementId
                + EXTENDED_DATA_FIELD_SEPARATOR
                + tableName
                + EXTENDED_DATA_FIELD_SEPARATOR
                + rowId
        );
    }

    private String createDocId(String s) {
        byte[] hash = Murmur3.hash128(s.getBytes());
        return Ascii85.encode(hash);
    }

    public String createMetadataDocId(String key) {
        byte[] hash = Murmur3.hash128(("__metadata_" + key).getBytes());
        return Ascii85.encode(hash);
    }

    public String createMutationDocId(ElementLocation elementLocation, Mutation mutation) {
//        byte[] hash = Murmur3.hash128((
//            elementLocation.getElementType().name() + "_" + elementLocation.getId() + "_" + mutation.getTimestamp()
//        ).getBytes());
//        return Ascii85.encode(hash);
        return null;
    }

    public String vertexIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    public String edgeIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    private String elementIdFromSearchHit(SearchHit hit) {
        SearchHitField elementIdField = hit.getFields().get(FieldNames.ELEMENT_ID);
        if (elementIdField == null) {
            throw new VertexiumException("Could not find field: " + FieldNames.ELEMENT_ID);
        }
        return elementIdField.getValue();
    }

    public ExtendedDataRowId extendedDataRowIdFromSearchHit(SearchHit hit) {
        SearchHitField elementTypeField = hit.getFields().get(FieldNames.ELEMENT_TYPE);
        if (elementTypeField == null) {
            throw new VertexiumException("Could not find field: " + FieldNames.ELEMENT_TYPE);
        }
        ElementType elementType = ElasticsearchDocumentType.parse(elementTypeField.getValue().toString()).toElementType();

        SearchHitField elementIdField = hit.getFields().get(FieldNames.ELEMENT_ID);
        if (elementIdField == null) {
            throw new VertexiumException("Could not find field: " + FieldNames.ELEMENT_ID);
        }
        String elementId = elementIdField.getValue();

        SearchHitField tableNameField = hit.getFields().get(FieldNames.EXTENDED_DATA_TABLE_NAME);
        if (tableNameField == null) {
            throw new VertexiumException("Could not find field: " + FieldNames.EXTENDED_DATA_TABLE_NAME);
        }
        String tableName = tableNameField.getValue();

        SearchHitField rowIdField = hit.getFields().get(FieldNames.EXTENDED_DATA_TABLE_ROW_ID);
        if (rowIdField == null) {
            throw new VertexiumException("Could not find field: " + FieldNames.EXTENDED_DATA_TABLE_ROW_ID);
        }
        String rowId = rowIdField.getValue();

        return new ExtendedDataRowId(elementType, elementId, tableName, rowId);
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
