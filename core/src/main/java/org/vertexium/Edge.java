package org.vertexium;

import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.util.IterableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface Edge extends Element {
    /**
     * Meta property name used for sorting and aggregations
     */
    String LABEL_PROPERTY_NAME = "__edgeLabel";

    /**
     * Meta property name used for sorting and aggregations
     */
    String OUT_VERTEX_ID_PROPERTY_NAME = "__outVertexId";

    /**
     * Meta property name used for sorting and aggregations
     */
    String IN_VERTEX_ID_PROPERTY_NAME = "__inVertexId";

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
    default Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getVertex(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    /**
     * Get the attach vertex on either side of the edge.
     *
     * @param direction  The side of the edge to get the vertex from (IN or OUT).
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @return The vertex.
     */
    default Vertex getVertex(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        String vertexId = getVertexId(direction);
        return getGraph().getVertex(vertexId, fetchHints, authorizations);
    }

    /**
     * Given a vertexId that represents one side of a relationship, get me the id of the other side.
     */
    String getOtherVertexId(String myVertexId);

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    default Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, getGraph().getDefaultFetchHints(), authorizations);
    }

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    default Vertex getOtherVertex(String myVertexId, FetchHints fetchHints, Authorizations authorizations) {
        String vertexId = getOtherVertexId(myVertexId);
        return getGraph().getVertex(vertexId, fetchHints, authorizations);
    }

    /**
     * Gets both in and out vertices of this edge.
     */
    default EdgeVertices getVertices(Authorizations authorizations) {
        return getVertices(getGraph().getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets both in and out vertices of this edge.
     */
    default EdgeVertices getVertices(FetchHints fetchHints, Authorizations authorizations) {
        List<String> ids = new ArrayList<>();
        ids.add(getVertexId(Direction.OUT));
        ids.add(getVertexId(Direction.IN));
        Map<String, Vertex> vertices = IterableUtils.toMapById(getGraph().getVertices(ids, fetchHints, authorizations));
        return new EdgeVertices(
                vertices.get(getVertexId(Direction.OUT)),
                vertices.get(getVertexId(Direction.IN))
        );
    }

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareEdge(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ExistingEdgeMutation prepareMutation();
}
