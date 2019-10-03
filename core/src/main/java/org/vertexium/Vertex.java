package org.vertexium;

import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.VertexQuery;

@SuppressWarnings("unchecked")
public interface Vertex extends Element {
    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, Authorizations authorizations);

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets a list of edge ids between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex to find edges on.
     * @param direction      The direction of the edge.
     * @param labels         The labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids.
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets edge summary information
     *
     * @param authorizations The authorizations used to get the edges summary
     * @return The edges summary
     */
    EdgesSummary getEdgesSummary(Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param label          The label of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param labels         The labels of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, Long endTime, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param queryString    The string to search for.
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(String queryString, Authorizations authorizations);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareVertex(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    @Override
    ExistingElementMutation<Vertex> prepareMutation();

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    @Override
    default ElementType getElementType() {
        return ElementType.VERTEX;
    }
}
