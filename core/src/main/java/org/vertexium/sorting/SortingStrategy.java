package org.vertexium.sorting;

import org.vertexium.VertexiumObject;
import org.vertexium.query.SortDirection;

public interface SortingStrategy {
    /**
     * Compares two {@link VertexiumObject}.
     *
     * @return 0 if both objects are equal. If direction is {@link SortDirection#ASCENDING}: &gt;0 if o1 &gt; o2 or &lt;0 if
     * o1 &lt; o2. If direction is {@link SortDirection#DESCENDING}: &lt;0 if o1 &gt; o2 or &gt;0 if o1 &lt; o2.
     */
    int compare(VertexiumObject o1, VertexiumObject o2, SortDirection direction);
}
