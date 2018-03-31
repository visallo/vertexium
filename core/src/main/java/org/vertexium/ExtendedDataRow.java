package org.vertexium;

public interface ExtendedDataRow extends VertexiumObject {
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

    /**
     * Get the names of all the properties of this row.
     */
    Iterable<String> getPropertyNames();

    /**
     * Fetch hints used to get this row. {@code FetchHints.ALL} if this is a new row.
     */
    FetchHints getFetchHints();
}
