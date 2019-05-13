package org.vertexium;

import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.FutureDeprecation;

import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.toIterable;

public interface Vertex extends Element {
    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return toIterable(getEdges(direction, authorizations.getUser()));
    }

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param user      The user used to find the edges.
     * @return An Iterable of edges.
     */
    default Stream<Edge> getEdges(Direction direction, User user) {
        return getEdges(direction, getGraph().getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(direction, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edges.
     * @return An Iterable of edges.
     */
    default Stream<Edge> getEdges(Direction direction, FetchHints fetchHints, User user) {
        return getEdges(direction, fetchHints, null, user);
    }

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdges(direction, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeIds(direction, authorizations.getUser()));
    }

    /**
     * Gets the connected edge ids.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param user      The user used to find the edges.
     * @return An Iterable of edge ids
     */
    Stream<String> getEdgeIds(Direction direction, User user);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdges(direction, label, authorizations.getUser()));
    }

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param label     The edge label to search for.
     * @param user      The user used to find the edges.
     * @return An Iterable of edges.
     */
    default Stream<Edge> getEdges(Direction direction, String label, User user) {
        return getEdges(direction, label, getGraph().getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(direction, label, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param label      The edge label to search for.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, User user);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeIds(direction, label, authorizations.getUser()));
    }

    /**
     * Gets the connected edge ids.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param label     The edge label to search for.
     * @param user      The user used to find the edges.
     * @return An Iterable of edge ids
     */
    Stream<String> getEdgeIds(Direction direction, String label, User user);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdges(direction, labels, authorizations.getUser()));
    }

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param labels    An array of edge labels to search for.
     * @param user      The user used to find the edges.
     * @return An Iterable of edges.
     */
    default Stream<Edge> getEdges(Direction direction, String[] labels, User user) {
        return getEdges(direction, labels, getGraph().getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(direction, labels, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param labels     An array of edge labels to search for.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, User user);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeIds(direction, labels, authorizations.getUser()));
    }

    /**
     * Gets the connected edge ids.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param labels    An array of edge labels to search for.
     * @param user      The user used to find the edges.
     * @return An Iterable of edge ids
     */
    Stream<String> getEdgeIds(Direction direction, String[] labels, User user);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    default Stream<Edge> getEdges(Vertex otherVertex, Direction direction, User user) {
        return getEdges(otherVertex, direction, getGraph().getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param fetchHints  Hint on what should be fetched from the datastore.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, User user);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, authorizations.getUser()));
    }

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param user        The user used to find the edges.
     * @return An Iterable of edge ids
     */
    Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, User user);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, label, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param label       The edge label to search for.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String label, User user);

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
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, label, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param label       The edge label to search for.
     * @param fetchHints  Hint on what should be fetched from the datastore.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, User user);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, label, authorizations.getUser()));
    }

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param label       The edge label to search for.
     * @param user        The user used to find the edges.
     * @return An Iterable of edge ids
     */
    Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, User user);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, labels, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param labels      An array of edge labels to search for.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, User user);

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
    @FutureDeprecation
    default Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, labels, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex The other vertex.
     * @param direction   The direction of edges to find relative to this vertex.
     * @param labels      An array of edge labels to search for.
     * @param fetchHints  Hint on what should be fetched from the datastore.
     * @param user        The user used to find the edges.
     * @return An Iterable of edges.
     */
    Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, User user);

    /**
     * Gets a list of edge ids between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex to find edges on.
     * @param direction      The direction of the edge.
     * @param labels         The labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids.
     */
    @FutureDeprecation
    default Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, labels, authorizations.getUser()));
    }

    /**
     * Gets a list of edge ids between this vertex and another vertex.
     *
     * @param otherVertex The other vertex to find edges on.
     * @param direction   The direction of the edge.
     * @param labels      The labels to search for.
     * @param user        The user used to find the edges.
     * @return An Iterable of edge ids.
     */
    Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, User user);

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
    @FutureDeprecation
    default Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeInfos(direction, authorizations.getUser()));
    }

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction The direction of the edge.
     * @param user      The user used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    default Stream<EdgeInfo> getEdgeInfos(Direction direction, User user) {
        return getEdgeInfos(direction, (String[]) null, user);
    }

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param label          The label of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    @FutureDeprecation
    default Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeInfos(direction, label, authorizations.getUser()));
    }

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction The direction of the edge.
     * @param label     The label of edges to traverse to find the edge infos.
     * @param user      The user used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Stream<EdgeInfo> getEdgeInfos(Direction direction, String label, User user);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param labels         The labels of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    @FutureDeprecation
    default Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeInfos(direction, labels, authorizations.getUser()));
    }

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction The direction of the edge.
     * @param labels    The labels of edges to traverse to find the edge infos.
     * @param user      The user used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, User user);


    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return toIterable(getVertices(direction, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction The direction relative to this vertex.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVertices(direction, fetchHints, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction  The direction relative to this vertex.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, FetchHints fetchHints, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getVertices(direction, label, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction The direction relative to this vertex.
     * @param label     The label of edges to traverse to find the vertices.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String label, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String label, Long endTime, Authorizations authorizations) {
        return toIterable(getVertices(direction, label, endTime, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction The direction relative to this vertex.
     * @param label     The label of edges to traverse to find the vertices.
     * @param endTime   Include all changes made up until the point in time.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String label, Long endTime, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVertices(direction, label, fetchHints, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction  The direction relative to this vertex.
     * @param label      The label of edges to traverse to find the vertices.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, User user);

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
    @FutureDeprecation
    Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction  The direction relative to this vertex.
     * @param label      The label of edges to traverse to find the vertices.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getVertices(direction, labels, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction The direction relative to this vertex.
     * @param labels    The labels of edges to traverse to find the vertices.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String[] labels, User user);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVertices(direction, labels, fetchHints, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction  The direction relative to this vertex.
     * @param labels     The labels of edges to traverse to find the vertices.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, User user);

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
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getVertices(direction, labels, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction  The direction relative to this vertex.
     * @param labels     The labels of edges to traverse to find the vertices.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user used to find the vertices.
     * @return An Iterable of vertices.
     */
    Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    @FutureDeprecation
    default Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getVertexIds(direction, label, authorizations.getUser()));
    }

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction The direction relative to this vertex.
     * @param label     The label of edges to traverse to find the vertices.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Stream<String> getVertexIds(Direction direction, String label, User user);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    @FutureDeprecation
    default Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getVertexIds(direction, labels, authorizations.getUser()));
    }

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction The direction relative to this vertex.
     * @param labels    The labels of edges to traverse to find the vertices.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Stream<String> getVertexIds(Direction direction, String[] labels, User user);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    @FutureDeprecation
    default Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return toIterable(getVertexIds(direction, authorizations.getUser()));
    }

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction The direction relative to this vertex.
     * @param user      The user used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Stream<String> getVertexIds(Direction direction, User user);

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
    ExistingElementMutation<Vertex> prepareMutation();

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param user      The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, User user);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, User user);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, label, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param label     The edge label to search for.
     * @param user      The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, User user);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, label, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param label      The edge label to search for.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, User user);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, labels, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction The side of the edge that this vertex is attached to.
     * @param labels    An array of edge labels to search for.
     * @param user      The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, User user);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    @FutureDeprecation
    default Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(direction, labels, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction  The side of the edge that this vertex is attached to.
     * @param labels     An array of edge labels to search for.
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @param user       The user used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, User user);
}
