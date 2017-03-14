package org.vertexium.query;

import org.vertexium.ElementType;

public class HasExtendedDataFilter {
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;

    public HasExtendedDataFilter(ElementType elementType, String elementId, String tableName) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
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

    @Override
    public String toString() {
        return "HasExtendedDataFilter{" +
                "elementType=" + elementType +
                ", elementId='" + elementId + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
