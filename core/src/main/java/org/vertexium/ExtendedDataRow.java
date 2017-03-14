package org.vertexium;

public interface ExtendedDataRow extends VertexiumObject {
    /**
     * Id of the row
     */
    ExtendedDataRowId getId();

    /**
     * Get the names of all the properties of this row.
     */
    Iterable<String> getPropertyNames();
}
