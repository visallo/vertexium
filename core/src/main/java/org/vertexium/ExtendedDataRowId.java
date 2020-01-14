package org.vertexium;

import java.io.Serializable;

public class ExtendedDataRowId implements Serializable, Comparable<ExtendedDataRowId>, VertexiumObjectId {
    private static final long serialVersionUID = 6419674145598605844L;
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;
    private final String rowId;

    public ExtendedDataRowId(ElementType elementType, String elementId, String tableName, String rowId) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
        this.rowId = rowId;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExtendedDataRowId that = (ExtendedDataRowId) o;

        if (elementType != that.elementType) {
            return false;
        }
        if (!elementId.equals(that.elementId)) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        return rowId.equals(that.rowId);
    }

    @Override
    public int hashCode() {
        int result = elementType.hashCode();
        result = 31 * result + elementId.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + rowId.hashCode();
        return result;
    }

    @Override
    public int compareTo(ExtendedDataRowId other) {
        int i = this.getElementType().compareTo(other.getElementType());
        if (i != 0) {
            return i;
        }

        i = this.getElementId().compareTo(other.getElementId());
        if (i != 0) {
            return i;
        }

        i = this.getTableName().compareTo(other.getTableName());
        if (i != 0) {
            return i;
        }

        i = this.getRowId().compareTo(other.getRowId());
        if (i != 0) {
            return i;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "ExtendedDataRowId{" +
            "elementType=" + elementType +
            ", elementId='" + elementId + '\'' +
            ", tableName='" + tableName + '\'' +
            ", rowId='" + rowId + '\'' +
            '}';
    }
}
