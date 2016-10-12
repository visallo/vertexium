package org.vertexium;

import org.vertexium.mutation.ExistingEdgeMutation;

import java.util.EnumSet;

public interface Edge extends Element {
    /**
     * Meta property name used for sorting and aggregations
     */
    String LABEL_PROPERTY_NAME = "__edgeLabel";

    /**
     * The edge label.
     */
    String getLabel();

    /**
     * Get the attach vertex id on either side of the edge.
     *
     * @param direction The side of the edge to get the vertex id from (IN or OUT).
     * @return The id of the vertex.
     */
    String getVertexId(Direction direction);

    /**
     * Get the attach vertex on either side of the edge.
     *
     * @param direction The side of the edge to get the vertex from (IN or OUT).
     * @return The vertex.
     */
    Vertex getVertex(Direction direction, Authorizations authorizations);

    /**
     * Get the attach vertex on either side of the edge.
     *
     * @param direction  The side of the edge to get the vertex from (IN or OUT).
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @return The vertex.
     */
    Vertex getVertex(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Given a vertexId that represents one side of a relationship, get me the id of the other side.
     */
    String getOtherVertexId(String myVertexId);

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    Vertex getOtherVertex(String myVertexId, Authorizations authorizations);

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    Vertex getOtherVertex(String myVertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Gets both in and out vertices of this edge.
     */
    EdgeVertices getVertices(Authorizations authorizations);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareEdge(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ExistingEdgeMutation prepareMutation();
}
