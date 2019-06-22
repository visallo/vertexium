package org.vertexium.sorting;

import org.vertexium.VertexiumObject;
import org.vertexium.query.SortDirection;

public interface SortingStrategy {
    /**
     * Compares two {@link VertexiumObject}.
     *
     * @return 0 if both objects are equal. If direction is {@link SortDirection#ASCENDING}: >0 if o1 > o2 or <0 if
     * o1 < o2. If direction is {@link SortDirection#DESCENDING}: <0 if o1 > o2 or >0 if o1 < o2.
     */
    int compare(VertexiumObject o1, VertexiumObject o2, SortDirection direction);
}
