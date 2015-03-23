package org.neolumin.vertexium;

import org.neolumin.vertexium.event.GraphEventListener;
import org.neolumin.vertexium.id.IdGenerator;
import org.neolumin.vertexium.query.GraphQuery;
import org.neolumin.vertexium.query.MultiVertexQuery;
import org.neolumin.vertexium.query.SimilarToGraphQuery;

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
     * with a single operation.
     *
     * @param vertexId   The id to assign the new vertex.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(String vertexId, Visibility visibility);

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
    Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

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
    Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Tests the existence of vertices with the given authorizations.
     *
     * @param ids            The vertex ids to check existence of.
     * @param authorizations The authorizations required to load the vertices.
     * @return Map of ids to exists status.
     */
    Map<String, Boolean> doVerticesExist(List<String> ids, Authorizations authorizations);

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
    Iterable<Vertex> getVertices(Iterable<String> ids, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

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
    List<Vertex> getVerticesInOrder(Iterable<String> ids, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Removes a vertex from the graph.
     *
     * @param vertex         The vertex to remove.
     * @param authorizations The authorizations required to remove the vertex.
     */
    void removeVertex(Vertex vertex, Authorizations authorizations);

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
    Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

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
    Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    Map<String, Boolean> doEdgesExist(List<String> ids, Authorizations authorizations);

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
    Iterable<Edge> getEdges(Iterable<String> ids, EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations);

    /**
     * Removes an edge from the graph.
     *
     * @param edge           The edge to remove.
     * @param authorizations The authorizations required to remove the edge.
     */
    void removeEdge(Edge edge, Authorizations authorizations);

    /**
     * Removes an edge from the graph.
     *
     * @param edgeId         The edge id of the vertex to remove from the graph.
     * @param authorizations The authorizations required to remove the edge.
     */
    void removeEdge(String edgeId, Authorizations authorizations);

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
     * @param sourceVertex   The source vertex to start the search from.
     * @param destVertex     The destination vertex to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertex     The source vertex to start the search from.
     * @param destVertex       The destination vertex to get to.
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, ProgressCallback progressCallback, Authorizations authorizations);

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId   The source vertex id to start the search from.
     * @param destVertexId     The destination vertex id to get to.
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, ProgressCallback progressCallback, Authorizations authorizations);

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
     * Creates a defines property builder. This is typically used by the indexer to give it hints on how it should index a property.
     *
     * @param propertyName The name of the property to define.
     */
    DefinePropertyBuilder defineProperty(String propertyName);

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
     * Determine if field boost is support. That is can you change the boost at a field level to give higher priority.
     */
    boolean isFieldBoostSupported();

    /**
     * Clears all data from the graph.
     */
    void clearData();

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
}
