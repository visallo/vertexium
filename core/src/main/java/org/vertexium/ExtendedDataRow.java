package org.vertexium;

public interface ExtendedDataRow extends VertexiumObject {
    /**
     * Meta property name used for sorting
     */
    String ELEMENT_ID = "__extendedDataElementId";

    /**
     * Meta property name used for sorting
     */
    String ELEMENT_TYPE = "__extendedDataElementType";

    /**
     * Meta property name used for sorting and aggregations
     */
    String TABLE_NAME = "__extendedDataTableName";

    /**
     * Meta property name used for sorting
     */
    String ROW_ID = "__extendedDataRowId";

    /**
     * Id of the row
     */
    ExtendedDataRowId getId();
}
