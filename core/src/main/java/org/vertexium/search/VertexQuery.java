package org.vertexium.search;

import org.vertexium.Direction;

public interface VertexQuery extends Query {
    /**
     * Sets the direction of edges to search
     */
    VertexQuery hasDirection(Direction direction);

    /**
     * Limits the search to only those edges with otherVertexId on the other end of the edge.
     */
    VertexQuery hasOtherVertexId(String otherVertexId);
}
