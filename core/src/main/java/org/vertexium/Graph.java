package org.vertexium;

import org.vertexium.event.GraphEventListener;
import org.vertexium.id.IdGenerator;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;

import java.io.InputStream;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public interface Graph {
    /**
     * Adds a vertex to the graph. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     */
    Vertex addVertex(Visibility visibility, Authorizations authorizations);

    /**
     * Adds a vertex to the graph.
     *
     * @param vertexId       The id to assign the new vertex.
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     */
    Vertex addVertex(String vertexId, Visibility visibility, Authorizations authorizations);

    /**
     * Adds the vertices to the graph.
     *
     * @param vertices       The vertices to add.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The vertices.
     */
    Iterable<Vertex> addVertices(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations);

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(Visibility visibility);

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param timestamp  The timestamp of the vertex. null, to use the system generated time.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(Long timestamp, Visibility visibility);

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation.
     *
     * @param vertexId   The id to assign the new vertex.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(String vertexId, Visibility visibility);

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation.
     *
     * @param vertexId   The id to assign the new vertex.
     * @param timestamp  The timestamp of the vertex.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility);

    /**
     * Tests the existence of a vertex with the given authorizations.
     *
     * @param vertexId       The vertex id to check existence of.
     * @param authorizations The authorizations required to load the vertex.
     * @return True if vertex exists.
     */
    boolean doesVertexExist(String vertexId, Authorizations authorizations);

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Vertex getVertex(String vertexId, Authorizations authorizations);

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Vertex getVertex(String vertexId, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, Authorizations authorizations);

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    Iterable<Vertex> getVerticesInRange(Range idRange, Authorizations authorizations);

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets all vertices on the graph.
     *
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(Authorizations authorizations);

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Tests the existence of vertices with the given authorizations.
     *
     * @param ids            The vertex ids to check existence of.
     * @param authorizations The authorizations required to load the vertices.
     * @return Map of ids to exists status.
     */
    Map<String, Boolean> doVerticesExist(Iterable<String> ids, Authorizations authorizations);

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(Iterable<String> ids, Authorizations authorizations);

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids            The ids of the vertices to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations);

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    List<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertex         The vertex to delete.
     * @param authorizations The authorizations required to delete the vertex.
     */
    void deleteVertex(Vertex vertex, Authorizations authorizations);

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to delete.
     * @param authorizations The authorizations required to delete the vertex.
     */
    void deleteVertex(String vertexId, Authorizations authorizations);

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        softDeleteVertex(vertex, (Object) null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    void softDeleteVertex(Vertex vertex, Object data, Authorizations authorizations);

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        softDeleteVertex(vertex, timestamp, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    void softDeleteVertex(Vertex vertex, Long timestamp, Object data, Authorizations authorizations);

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(String vertexId, Authorizations authorizations) {
        softDeleteVertex(vertexId, (Object) null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    void softDeleteVertex(String vertexId, Object data, Authorizations authorizations);

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(String vertexId, Long timestamp, Authorizations authorizations) {
        softDeleteVertex(vertexId, timestamp, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    void softDeleteVertex(String vertexId, Long timestamp, Object data, Authorizations authorizations);

    /**
     * Adds an edge between two vertices. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param outVertex      The source vertex. The "out" side of the edge.
     * @param inVertex       The destination vertex. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations);

    /**
     * Adds an edge between two vertices.
     *
     * @param edgeId         The id to assign the new edge.
     * @param outVertex      The source vertex. The "out" side of the edge.
     * @param inVertex       The destination vertex. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations);

    /**
     * Adds an edge between two vertices.
     *
     * @param outVertexId    The source vertex id. The "out" side of the edge.
     * @param inVertexId     The destination vertex id. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    Edge addEdge(String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations);

    /**
     * Adds an edge between two vertices.
     *
     * @param edgeId         The id to assign the new edge.
     * @param outVertexId    The source vertex id. The "out" side of the edge.
     * @param inVertexId     The destination vertex id. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    Edge addEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation. The id of the new edge will be generated using an IdGenerator.
     *
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId     The id to assign the new edge.
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId     The id to assign the new edge.
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param timestamp  The timestamp of the edge.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilderByVertexId prepareEdge(String outVertexId, String inVertexId, String label, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId      The id to assign the new edge.
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId      The id to assign the new edge.
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param timestamp   The timestamp of the edge.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    /**
     * Tests the existence of a edge with the given authorizations.
     *
     * @param edgeId         The edge id to check existence of.
     * @param authorizations The authorizations required to load the edge.
     * @return True if edge exists.
     */
    boolean doesEdgeExist(String edgeId, Authorizations authorizations);

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    Edge getEdge(String edgeId, Authorizations authorizations);

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    Edge getEdge(String edgeId, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets all edges on the graph.
     *
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(Authorizations authorizations);

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    Iterable<Edge> getEdgesInRange(Range idRange, Authorizations authorizations);

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Filters a collection of edge ids by the authorizations of that edge, properties, etc. If
     * any of the filtered items match that edge id will be included.
     *
     * @param edgeIds              The edge ids to filter on.
     * @param authorizationToMatch The authorization to look for
     * @param filters              The parts of the edge to filter on
     * @param authorizations       The authorization to find the edges with
     * @return The filtered down list of edge ids
     */
    Iterable<String> filterEdgeIdsByAuthorization(Iterable<String> edgeIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations);

    /**
     * Filters a collection of vertex ids by the authorizations of that vertex, properties, etc. If
     * any of the filtered items match that vertex id will be included.
     *
     * @param vertexIds            The vertex ids to filter on.
     * @param authorizationToMatch The authorization to look for
     * @param filters              The parts of the edge to filter on
     * @param authorizations       The authorization to find the edges with
     * @return The filtered down list of vertex ids
     */
    Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations);

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    Map<String, Boolean> doEdgesExist(Iterable<String> ids, Authorizations authorizations);

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, Authorizations authorizations);

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(Iterable<String> ids, Authorizations authorizations);

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Use {@link #findRelatedEdgeIds(Iterable, Authorizations)}
     */
    @Deprecated
    Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations);

    /**
     * Use {@link #findRelatedEdgeIds(Iterable, Long, Authorizations)}
     */
    @Deprecated
    Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Long endTime, Authorizations authorizations);

    /**
     * Given a list of vertices, find all the edge ids that connect them.
     *
     * @param vertices       The list of vertices.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    Iterable<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> vertices, Authorizations authorizations);

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Authorizations authorizations);

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations);

    /**
     * Given a list of vertices, find all the edges that connect them.
     *
     * @param vertices       The list of vertices.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    Iterable<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> vertices, Authorizations authorizations);

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Authorizations authorizations);

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations);

    /**
     * Permanently deletes an edge from the graph.
     *
     * @param edge           The edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    void deleteEdge(Edge edge, Authorizations authorizations);

    /**
     * Permanently deletes an edge from the graph. This method requires fetching the edge before deletion.
     *
     * @param edgeId         The edge id of the edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    void deleteEdge(String edgeId, Authorizations authorizations);

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(Edge edge, Authorizations authorizations) {
        softDeleteEdge(edge, (Object) null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     */
    void softDeleteEdge(Edge edge, Object data, Authorizations authorizations);

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        softDeleteEdge(edge, timestamp, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     */
    void softDeleteEdge(Edge edge, Long timestamp, Object data, Authorizations authorizations);

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(String edgeId, Authorizations authorizations) {
        softDeleteEdge(edgeId, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     */
    void softDeleteEdge(String edgeId, Object data, Authorizations authorizations);

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(String edgeId, Long timestamp, Authorizations authorizations) {
        softDeleteEdge(edgeId, timestamp, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param data           Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     */
    void softDeleteEdge(String edgeId, Long timestamp, Object data, Authorizations authorizations);

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    GraphQuery query(String queryString, Authorizations authorizations);

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    GraphQuery query(Authorizations authorizations);

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations);

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    MultiVertexQuery query(String[] vertexIds, Authorizations authorizations);

    /**
     * Returns true if this graph supports similar to text queries.
     */
    boolean isQuerySimilarToTextSupported();

    /**
     * Creates a query builder object that finds all vertices similar to the given text for the specified fields.
     * This could be implemented similar to the ElasticSearch more like this query.
     *
     * @param fields         The fields to match against.
     * @param text           The text to find similar to.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations);

    /**
     * Flushes any pending mutations to the graph.
     */
    void flush();

    /**
     * Cleans up or disconnects from the underlying storage.
     */
    void shutdown();

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param labels         Edge labels
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId   The source vertex id to start the search from.
     * @param destVertexId     The destination vertex id to get to.
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, ProgressCallback progressCallback, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId   The source vertex id to start the search from.
     * @param destVertexId     The destination vertex id to get to.
     * @param labels           Edge labels
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, ProgressCallback progressCallback, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param options        Find path options
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations);

    /**
     * Gets the id generator used by this graph to create ids.
     *
     * @return the id generator.
     */
    IdGenerator getIdGenerator();

    /**
     * Given an authorization is the visibility object valid.
     *
     * @param visibility     The visibility you want to check.
     * @param authorizations The given authorizations.
     * @return true if the visibility is valid given an authorization, else return false.
     */
    boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    /**
     * Reindex all vertices and edges.
     *
     * @param authorizations authorizations used to query for the data to reindex.
     */
    void reindex(Authorizations authorizations);

    /**
     * Sets metadata on the graph.
     *
     * @param key   The key to the metadata.
     * @param value The value to set.
     */
    void setMetadata(String key, Object value);

    /**
     * Gets metadata from the graph.
     *
     * @param key The key to the metadata.
     * @return The metadata value, or null.
     */
    Object getMetadata(String key);

    /**
     * Gets all metadata.
     *
     * @return Iterable of all metadata.
     */
    Iterable<GraphMetadataEntry> getMetadata();

    /**
     * Gets all metadata with the given prefix.
     */
    Iterable<GraphMetadataEntry> getMetadataWithPrefix(String prefix);

    /**
     * Determine if field boost is support. That is can you change the boost at a field level to give higher priority.
     */
    boolean isFieldBoostSupported();

    /**
     * Clears all data from the graph.
     */
    void truncate();

    /**
     * Drops all tables
     */
    void drop();

    /**
     * Gets the granularity of the search index {@link SearchIndexSecurityGranularity}
     */
    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    /**
     * Adds a graph event listener that will be called when graph events occur.
     */
    void addGraphEventListener(GraphEventListener graphEventListener);

    /**
     * Marks a vertex as hidden for a given visibility.
     *
     * @param vertex         The vertex to mark hidden.
     * @param visibility     The visibility string under which this vertex is hidden.
     *                       This visibility can be a superset of the vertex visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a vertex as visible for a given visibility, effectively undoing markVertexHidden.
     *
     * @param vertex         The vertex to mark visible.
     * @param visibility     The visibility string under which this vertex is now visible.
     * @param authorizations The authorizations used.
     */
    void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations);

    /**
     * Marks an edge as hidden for a given visibility.
     *
     * @param edge           The edge to mark hidden.
     * @param visibility     The visibility string under which this edge is hidden.
     *                       This visibility can be a superset of the edge visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations);

    /**
     * Marks an edge as visible for a given visibility, effectively undoing markEdgeHidden.
     *
     * @param edge           The edge to mark visible.
     * @param visibility     The visibility string under which this edge is now visible.
     * @param authorizations The authorizations used.
     */
    void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations);

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(String... auths);

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(Collection<String> auths);

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations);

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations);

    /**
     * Gets the number of times a property with a given value occurs on vertices
     *
     * @param propertyName   The name of the property to find
     * @param authorizations The authorizations to use to find the property
     * @return The results
     */
    Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations);

    /**
     * Gets a count of the number of vertices in the system.
     */
    long getVertexCount(Authorizations authorizations);

    /**
     * Gets a count of the number of edges in the system.
     */
    long getEdgeCount(Authorizations authorizations);

    /**
     * Save a pre-made property definition.
     *
     * @param propertyDefinition the property definition to save.
     */
    void savePropertyDefinition(PropertyDefinition propertyDefinition);

    /**
     * Creates a defines property builder. This is typically used by the indexer to give it hints on how it should index a property.
     *
     * @param propertyName The name of the property to define.
     */
    DefinePropertyBuilder defineProperty(String propertyName);

    /**
     * Determine if a property is already defined
     */
    boolean isPropertyDefined(String propertyName);

    /**
     * Gets the property definition for the given name.
     *
     * @param propertyName name of the property
     * @return the property definition if found. null otherwise.
     */
    PropertyDefinition getPropertyDefinition(String propertyName);

    /**
     * Gets all property definitions.
     *
     * @return all property definitions.
     */
    Collection<PropertyDefinition> getPropertyDefinitions();

    /**
     * Saves multiple mutations with a single call.
     *
     * @param mutations      the mutations to save
     * @param authorizations the authorizations used during save
     * @return the elements which were saved
     */
    Iterable<Element> saveElementMutations(Iterable<ElementMutation> mutations, Authorizations authorizations);

    /**
     * Opens multiple StreamingPropertyValue input streams at once. This can have performance benefits by
     * reducing the number of queries to the underlying data source.
     *
     * @param streamingPropertyValues list of StreamingPropertyValues to get input streams for
     * @return InputStreams in the same order as the input list
     */
    List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues);

    /**
     * Gets the specified extended data rows.
     *
     * @param ids            The ids of the rows to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, Authorizations authorizations);

    /**
     * Gets the specified extended data row.
     *
     * @param id             The id of the row to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations);

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    Iterable<ExtendedDataRow> getExtendedData(ElementType elementType, String elementId, String tableName, Authorizations authorizations);

    /**
     * Gets extended data rows from the graph in the given range.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementIdRange The range of element ids to get extended data rows for.
     * @param authorizations The authorizations required to load the vertex.
     * @return The extended data rows for the element ids in the range.
     */
    Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, Authorizations authorizations);

    /**
     * Deletes an extended data row
     */
    void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations);

    /**
     * The default fetch hints to use if none are provided
     */
    FetchHints getDefaultFetchHints();

    /**
     * Visits all elements on the graph
     */
    void visitElements(GraphVisitor graphVisitor, Authorizations authorizations);

    /**
     * Visits all vertices on the graph
     */
    void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations);

    /**
     * Visits all edges on the graph
     */
    void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations);

    /**
     * Visits elements using the supplied elements and visitor
     */
    void visit(Iterable<? extends Element> elements, GraphVisitor visitor);
}
