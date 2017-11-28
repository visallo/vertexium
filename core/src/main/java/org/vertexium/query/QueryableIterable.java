package org.vertexium.query;

import org.vertexium.Authorizations;
import org.vertexium.VertexiumObject;

public interface QueryableIterable<T extends VertexiumObject> extends Iterable<T> {
    /**
     * Creates a query for this iterable.
     *
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    Query query(Authorizations authorizations);

    /**
     * Creates a query for this iterable.
     *
     * @param queryString    The string to search for.
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    Query query(String queryString, Authorizations authorizations);
}
