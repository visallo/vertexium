package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.Element;
import org.vertexium.ElementType;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.elasticsearch5.ElasticsearchDocumentType;

public class ElasticsearchExtendedDataIdUtils {
    public static final String SEPARATOR = ":";

    public static String createForElement(Element element, String tableName, String rowId) {
        return create(element.getId(), tableName, rowId);
    }

    public static String toDocId(ExtendedDataRowId id) {
        return create(id.getElementId(), id.getTableName(), id.getRowId());
    }

    public static String create(String elementId, String tableName, String rowId) {
        return elementId + SEPARATOR + tableName + SEPARATOR + rowId;
    }

    public static ExtendedDataRowId fromSearchHit(SearchHit hit) {
        SearchHitField elementTypeField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        if (elementTypeField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        }
        ElementType elementType = ElasticsearchDocumentType.parse(elementTypeField.getValue().toString()).toElementType();

        SearchHitField elementIdField = hit.getFields().get(Elasticsearch5SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME);
        if (elementIdField == null) {
            throw new VertexiumException("Could not find field: " + Elasticsearch5SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME);
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
}
