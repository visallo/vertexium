package org.vertexium.query;

import org.vertexium.ElementType;

public class HasExtendedDataFilter {
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;
    private final String rowId;

    public HasExtendedDataFilter(ElementType elementType, String elementId, String tableName, String rowId) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
        this.rowId = rowId;
    }

    public HasExtendedDataFilter(ElementType elementType, String elementId, String tableName) {
        this(elementType, elementId, tableName, null);
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getElementId() {
        return elementId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRowId() {
        return rowId;
    }

    @Override
    public String toString() {
        return "HasExtendedDataFilter{" +
            "elementType=" + elementType +
            ", elementId='" + elementId + '\'' +
            ", tableName='" + tableName + '\'' +
            ", rowId='" + rowId + '\'' +
            '}';
    }
}
