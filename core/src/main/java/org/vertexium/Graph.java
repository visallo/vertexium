package org.vertexium;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.vertexium.event.GraphEventListener;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.id.IdGenerator;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.*;
import org.vertexium.search.IndexHint;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.FutureDeprecation;
import org.vertexium.util.IterableUtils;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.StreamUtils.stream;
import static org.vertexium.util.StreamUtils.toIterable;

public interface Graph {
    /**
     * Adds a vertex to the graph. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     * @deprecated Use {@link #prepareVertex(Visibility)}
     */
    @Deprecated
    default Vertex addVertex(Visibility visibility, Authorizations authorizations) {
        return prepareVertex(visibility).save(authorizations);
    }

    /**
     * Adds a vertex to the graph.
     *
     * @param vertexId       The id to assign the new vertex.
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     * @deprecated Use {@link #prepareVertex(String, Visibility)}
     */
    @Deprecated
    default Vertex addVertex(String vertexId, Visibility visibility, Authorizations authorizations) {
        return prepareVertex(vertexId, visibility).save(authorizations);
    }

    /**
     * Adds the vertices to the graph.
     *
     * @param vertices       The vertices to add.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> addVertices(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations) {
        List<Vertex> addedVertices = new ArrayList<>();
        for (ElementBuilder<Vertex> vertexBuilder : vertices) {
            addedVertices.add(vertexBuilder.save(authorizations));
        }
        return addedVertices;
    }

    /**
     * Adds the vertices to the graph.
     *
     * @param vertices The vertices to add.
     * @param user     The user required to add and retrieve the new vertex.
     */
    default Stream<String> addVertices(Iterable<ElementBuilder<Vertex>> vertices, User user) {
        List<String> addedVertexIds = new ArrayList<>();
        for (ElementBuilder<Vertex> vertexBuilder : vertices) {
            addedVertexIds.add(vertexBuilder.save(user));
        }
        return addedVertexIds.stream();
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(Visibility visibility) {
        return prepareVertex(getIdGenerator().nextId(), null, visibility);
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param timestamp  The timestamp of the vertex. null, to use the system generated time.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(Long timestamp, Visibility visibility) {
        return prepareVertex(getIdGenerator().nextId(), timestamp, visibility);
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation.
     *
     * @param vertexId   The id to assign the new vertex.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        return prepareVertex(vertexId, null, visibility);
    }

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
    @FutureDeprecation
    default boolean doesVertexExist(String vertexId, Authorizations authorizations) {
        return doesVertexExist(vertexId, authorizations.getUser());
    }

    /**
     * Tests the existence of a vertex with the given authorizations.
     *
     * @param vertexId The vertex id to check existence of.
     * @param user     The user required to load the vertex.
     * @return True if vertex exists.
     */
    default boolean doesVertexExist(String vertexId, User user) {
        return getVertex(vertexId, FetchHints.NONE, user) != null;
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId      The element id to retrieve from the graph.
     * @param authorizations The authorizations required to load the element.
     * @return The element if successful. null if the element is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Element getElement(ElementId elementId, Authorizations authorizations) {
        return getElement(elementId, authorizations.getUser());
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId The element id to retrieve from the graph.
     * @param user      The user required to load the element.
     * @return The element if successful. null if the element is not found or the required authorizations were not provided.
     */
    default Element getElement(ElementId elementId, User user) {
        switch (elementId.getElementType()) {
            case EDGE:
                return getEdge(elementId.getElementId(), user);
            case VERTEX:
                return getVertex(elementId.getElementId(), user);
            default:
                throw new VertexiumException("Unhandled element type: " + elementId.getElementType());
        }
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId      The element id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the element to fetch.
     * @param authorizations The authorizations required to load the element.
     * @return The vertex if successful. null if the element is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Element getElement(ElementId elementId, FetchHints fetchHints, Authorizations authorizations) {
        return getElement(elementId, fetchHints, authorizations.getUser());
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId  The element id to retrieve from the graph.
     * @param fetchHints Hint at what parts of the element to fetch.
     * @param user       The user required to load the element.
     * @return The vertex if successful. null if the element is not found or the required authorizations were not provided.
     */
    default Element getElement(ElementId elementId, FetchHints fetchHints, User user) {
        switch (elementId.getElementType()) {
            case VERTEX:
                return getVertex(elementId.getElementId(), fetchHints, user);
            case EDGE:
                return getEdge(elementId.getElementId(), fetchHints, user);
            default:
                throw new VertexiumException("Unhandled element type: " + elementId.getElementType());
        }
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Vertex getVertex(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, authorizations.getUser());
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId The vertex id to retrieve from the graph.
     * @param user     The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Vertex getVertex(String vertexId, User user) {
        return getVertex(vertexId, getDefaultFetchHints(), user);
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Vertex getVertex(String vertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getVertex(vertexId, fetchHints, authorizations.getUser());
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId   The vertex id to retrieve from the graph.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Vertex getVertex(String vertexId, FetchHints fetchHints, User user) {
        return getVertex(vertexId, fetchHints, null, user);
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getVertex(vertexId, fetchHints, endTime, authorizations.getUser());
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId   The vertex id to retrieve from the graph.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, Authorizations authorizations) {
        return toIterable(getVerticesWithPrefix(vertexIdPrefix, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param user           The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, User user) {
        return getVerticesWithPrefix(vertexIdPrefix, getDefaultFetchHints(), user);
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVerticesWithPrefix(vertexIdPrefix, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param user           The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, User user) {
        return getVerticesWithPrefix(vertexIdPrefix, fetchHints, null, user);
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getVerticesWithPrefix(vertexIdPrefix, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param user           The user required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesInRange(Range idRange, Authorizations authorizations) {
        return toIterable(getVerticesInRange(idRange, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange The range of ids to get.
     * @param user    The user required to load the vertex.
     * @return The vertices in the range.
     */
    default Stream<Vertex> getVerticesInRange(Range idRange, User user) {
        return getVerticesInRange(idRange, getDefaultFetchHints(), user);
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVerticesInRange(idRange, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange    The range of ids to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return The vertices in the range.
     */
    default Stream<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, User user) {
        return getVerticesInRange(idRange, fetchHints, null, user);
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getVerticesInRange(idRange, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange    The range of ids to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the vertex.
     * @return The vertices in the range.
     */
    Stream<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets all vertices on the graph.
     *
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Authorizations authorizations) {
        return toIterable(getVertices(authorizations.getUser()));
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param user The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVertices(User user) {
        return getVertices(getDefaultFetchHints(), user);
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVertices(fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVertices(FetchHints fetchHints, User user) {
        return getVertices(fetchHints, null, user);
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getVertices(fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Stream<Vertex> getVertices(FetchHints fetchHints, Long endTime, User user);

    /**
     * Tests the existence of vertices with the given authorizations.
     *
     * @param ids            The vertex ids to check existence of.
     * @param authorizations The authorizations required to load the vertices.
     * @return Map of ids to exists status.
     */
    @FutureDeprecation
    default Map<String, Boolean> doVerticesExist(Iterable<String> ids, Authorizations authorizations) {
        return doVerticesExist(ids, authorizations.getUser());
    }

    /**
     * Tests the existence of vertices with the given authorizations.
     *
     * @param ids  The vertex ids to check existence of.
     * @param user The user required to load the vertices.
     * @return Map of ids to exists status.
     */
    default Map<String, Boolean> doVerticesExist(Iterable<String> ids, User user) {
        Map<String, Boolean> results = new HashMap<>();
        ids = Sets.newHashSet(ids);
        for (String id : ids) {
            results.put(id, false);
        }
        getVertices(ids, FetchHints.NONE, user)
            .forEach(vertex -> results.put(vertex.getId(), true));
        return results;
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Iterable<String> ids, Authorizations authorizations) {
        return toIterable(getVertices(ids, authorizations.getUser()));
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids  The ids of the vertices to get.
     * @param user The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVertices(Iterable<String> ids, User user) {
        return getVertices(ids, getDefaultFetchHints(), user);
    }

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
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getVertices(ids, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids        The ids of the vertices to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, User user) {
        return getVertices(ids, fetchHints, null, user);
    }

    /**
     * Gets all vertices matching the given ids on the graph mapped by their id.
     *
     * @param ids        The ids of the vertices to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return An map of vertices.
     */
    default Map<String, Vertex> getVerticesMappedById(Iterable<String> ids, FetchHints fetchHints, User user) {
        return getVerticesMappedById(ids, fetchHints, null, user);
    }

    /**
     * Gets all vertices matching the given ids on the graph mapped by their id.
     *
     * @param ids        The ids of the vertices to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return An map of vertices.
     */
    default Map<String, Vertex> getVerticesMappedById(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(ids, fetchHints, endTime, user)
            .collect(Collectors.toMap(Element::getId, v -> v));
    }

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
    @FutureDeprecation
    default Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getVertices(ids, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids        The ids of the vertices to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user);

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
    @FutureDeprecation
    default List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations) {
        return getVerticesInOrder(ids, authorizations.getUser()).collect(Collectors.toList());
    }

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids  The ids of the vertices to get.
     * @param user The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVerticesInOrder(Iterable<String> ids, User user) {
        return getVerticesInOrder(ids, getDefaultFetchHints(), user);
    }

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
    @FutureDeprecation
    default List<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesInOrder(ids, fetchHints, authorizations.getUser()).collect(Collectors.toList());
    }

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids        The ids of the vertices to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Stream<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, User user) {
        final List<String> vertexIds = IterableUtils.toList(ids);
        return getVertices(vertexIds, user)
            .sorted((v1, v2) -> {
                Integer i1 = vertexIds.indexOf(v1.getId());
                Integer i2 = vertexIds.indexOf(v2.getId());
                return i1.compareTo(i2);
            });
    }

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertex         The vertex to delete.
     * @param authorizations The authorizations required to delete the vertex.
     * @deprecated Use {@link ElementMutation#deleteElement()}
     */
    @Deprecated
    default void deleteVertex(Vertex vertex, Authorizations authorizations) {
        vertex.prepareMutation()
            .deleteElement()
            .save(authorizations);
    }

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to delete.
     * @param authorizations The authorizations required to delete the vertex.
     * @deprecated Use {@link ElementMutation#deleteElement()}
     */
    @Deprecated
    default void deleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to delete with id: " + vertexId);
        deleteVertex(vertex, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement()}
     */
    @Deprecated
    default void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        softDeleteVertex(vertex, (Object) null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Object)} )}
     */
    @Deprecated
    default void softDeleteVertex(Vertex vertex, Object eventData, Authorizations authorizations) {
        softDeleteVertex(vertex, null, eventData, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long, Object)} )}
     */
    @Deprecated
    default void softDeleteVertex(Vertex vertex, Long timestamp, Object eventData, Authorizations authorizations) {
        vertex.prepareMutation()
            .softDeleteElement(timestamp, eventData)
            .save(authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement()}
     */
    @Deprecated
    default void softDeleteVertex(String vertexId, Authorizations authorizations) {
        softDeleteVertex(vertexId, (Object) null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long)}
     */
    @Deprecated
    default void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        softDeleteVertex(vertex, timestamp, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Object)}
     */
    @Deprecated
    default void softDeleteVertex(String vertexId, Object eventData, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, null, eventData, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long)}
     */
    @Deprecated
    default void softDeleteVertex(String vertexId, Long timestamp, Authorizations authorizations) {
        softDeleteVertex(vertexId, timestamp, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to soft delete the vertex.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long, Object)}
     */
    @Deprecated
    default void softDeleteVertex(String vertexId, Long timestamp, Object eventData, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, timestamp, eventData, authorizations);
    }

    /**
     * Adds an edge between two vertices. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param outVertex      The source vertex. The "out" side of the edge.
     * @param inVertex       The destination vertex. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     * @deprecated Use {@link #prepareEdge(Vertex, Vertex, String, Visibility)}
     */
    @Deprecated
    default Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertex, inVertex, label, visibility).save(authorizations);
    }

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
     * @deprecated Use {@link #prepareEdge(String, Vertex, Vertex, String, Visibility)}
     */
    @Deprecated
    default Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility).save(authorizations);
    }

    /**
     * Adds an edge between two vertices.
     *
     * @param outVertexId    The source vertex id. The "out" side of the edge.
     * @param inVertexId     The destination vertex id. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     * @deprecated Use {@link #prepareEdge(String, String, String, Visibility)}
     */
    @Deprecated
    default Edge addEdge(String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertexId, inVertexId, label, visibility).save(authorizations);
    }

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
     * @deprecated Use {@link #prepareEdge(String, String, String, String, Visibility)}
     */
    @Deprecated
    default Edge addEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation. The id of the new edge will be generated using an IdGenerator.
     *
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     * @deprecated Use {@link #prepareEdge(String, String, String, Visibility)}
     */
    @Deprecated
    default EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility);
    }

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
    default EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

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
    default EdgeBuilderByVertexId prepareEdge(String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertexId, inVertexId, label, visibility);
    }

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
    default EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

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
    @FutureDeprecation
    default boolean doesEdgeExist(String edgeId, Authorizations authorizations) {
        return doesEdgeExist(edgeId, authorizations.getUser());
    }

    /**
     * Tests the existence of a edge with the given authorizations.
     *
     * @param edgeId The edge id to check existence of.
     * @param user   The user required to load the edge.
     * @return True if edge exists.
     */
    default boolean doesEdgeExist(String edgeId, User user) {
        return getEdge(edgeId, FetchHints.NONE, user) != null;
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Edge getEdge(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, authorizations.getUser());
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId The edge id to retrieve from the graph.
     * @param user   The user required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    default Edge getEdge(String edgeId, User user) {
        return getEdge(edgeId, getDefaultFetchHints(), user);
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Edge getEdge(String edgeId, FetchHints fetchHints, Authorizations authorizations) {
        return getEdge(edgeId, fetchHints, authorizations.getUser());
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId     The edge id to retrieve from the graph.
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param user       The user required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    default Edge getEdge(String edgeId, FetchHints fetchHints, User user) {
        return getEdge(edgeId, fetchHints, null, user);
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    @FutureDeprecation
    default Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getEdge(edgeId, fetchHints, endTime, authorizations.getUser());
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId     The edge id to retrieve from the graph.
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets all edges on the graph.
     *
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Authorizations authorizations) {
        return toIterable(getEdges(authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph.
     *
     * @param user The user required to load the edge.
     * @return An iterable of all the edges.
     */
    default Stream<Edge> getEdges(User user) {
        return getEdges(getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param user       The user required to load the edge.
     * @return An iterable of all the edges.
     */
    default Stream<Edge> getEdges(FetchHints fetchHints, User user) {
        return getEdges(fetchHints, null, user);
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdges(fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the edge.
     * @return An iterable of all the edges.
     */
    Stream<Edge> getEdges(FetchHints fetchHints, Long endTime, User user);

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdgesInRange(Range idRange, Authorizations authorizations) {
        return toIterable(getEdgesInRange(idRange, authorizations.getUser()));
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange The range of ids to get.
     * @param user    The user required to load the vertex.
     * @return The edges in the range.
     */
    default Stream<Edge> getEdgesInRange(Range idRange, User user) {
        return getEdgesInRange(idRange, getDefaultFetchHints(), user);
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgesInRange(idRange, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange    The range of ids to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param user       The user required to load the vertex.
     * @return The edges in the range.
     */
    default Stream<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, User user) {
        return getEdgesInRange(idRange, fetchHints, null, user);
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdgesInRange(idRange, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange    The range of ids to get.
     * @param fetchHints Hint at what parts of the vertex to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the vertex.
     * @return The edges in the range.
     */
    Stream<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user);

    /**
     * Filters a collection of edge ids by the authorizations of that edge, properties, etc. If
     * any of the filtered items match that edge id will be included.
     *
     * @param edgeIds              The edge ids to filter on.
     * @param authorizationToMatch The authorization to look for
     * @param filters              The parts of the edge to filter on
     * @param authorizations       The authorization to find the edges with
     * @return The filtered down list of edge ids
     * @deprecated Use {@link org.vertexium.query.Query#hasId(String...)} and {@link org.vertexium.query.Query#hasAuthorization(String...)}
     */
    @Deprecated
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
     * @deprecated Use {@link org.vertexium.query.Query#hasId(String...)} and {@link org.vertexium.query.Query#hasAuthorization(String...)}
     */
    @Deprecated
    Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations);

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    @FutureDeprecation
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, Authorizations authorizations) {
        return doEdgesExist(ids, authorizations.getUser());
    }

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids  The edge ids to check existence of.
     * @param user The user required to load the edges.
     * @return Maps of ids to exists status.
     */
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, User user) {
        return doEdgesExist(ids, null, user);
    }

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    @FutureDeprecation
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, Authorizations authorizations) {
        return doEdgesExist(ids, endTime, authorizations.getUser());
    }

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids     The edge ids to check existence of.
     * @param endTime Include all changes made up until the point in time.
     * @param user    The user required to load the edges.
     * @return Maps of ids to exists status.
     */
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, User user) {
        Map<String, Boolean> results = new HashMap<>();
        ids = Sets.newHashSet(ids);
        for (String id : ids) {
            results.put(id, false);
        }
        getEdges(ids, FetchHints.NONE, user)
            .forEach(edge -> results.put(edge.getId(), true));
        return results;
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Iterable<String> ids, Authorizations authorizations) {
        return toIterable(getEdges(ids, authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids  The ids of the edges to get.
     * @param user The user required to load the edge.
     * @return An iterable of all the edges.
     */
    default Stream<Edge> getEdges(Iterable<String> ids, User user) {
        return getEdges(ids, getDefaultFetchHints(), user);
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(ids, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids        The ids of the edges to get.
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param user       The user required to load the edge.
     * @return An iterable of all the edges.
     */
    default Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, User user) {
        return getEdges(ids, fetchHints, null, user);
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    @FutureDeprecation
    default Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdges(ids, fetchHints, endTime, authorizations.getUser()));
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids        The ids of the edges to get.
     * @param fetchHints Hint at what parts of the edge to fetch.
     * @param endTime    Include all changes made up until the point in time.
     * @param user       The user required to load the edge.
     * @return An iterable of all the edges.
     */
    Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user);

    /**
     * Given a list of vertices, find all the edge ids that connect them.
     *
     * @param vertices       The list of vertices.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    @FutureDeprecation
    default Iterable<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> vertices, Authorizations authorizations) {
        return toIterable(findRelatedEdgeIdsForVertices(vertices, authorizations.getUser()));
    }

    /**
     * Given a list of vertices, find all the edge ids that connect them.
     *
     * @param vertices The list of vertices.
     * @param user     The user required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    default Stream<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> vertices, User user) {
        List<String> results = new ArrayList<>();
        List<Vertex> verticesList = IterableUtils.toList(vertices);
        for (Vertex outVertex : verticesList) {
            if (outVertex == null) {
                throw new VertexiumException("verticesIterable cannot have null values");
            }
            outVertex.getEdgeInfos(Direction.OUT, user)
                .forEach(edgeInfo -> {
                    for (Vertex inVertex : verticesList) {
                        if (edgeInfo.getVertexId() == null) { // This check is for legacy data. null EdgeInfo.vertexIds are no longer permitted
                            continue;
                        }
                        if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                            results.add(edgeInfo.getEdgeId());
                        }
                    }
                });
        }
        return results.stream();
    }

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    @FutureDeprecation
    default Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Authorizations authorizations) {
        return toIterable(findRelatedEdgeIds(vertexIds, authorizations.getUser()));
    }

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds The list of vertex ids.
     * @param user      The user required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    default Stream<String> findRelatedEdgeIds(Iterable<String> vertexIds, User user) {
        return findRelatedEdgeIds(vertexIds, null, user);
    }

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    @FutureDeprecation
    default Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        return toIterable(findRelatedEdgeIds(vertexIds, endTime, authorizations.getUser()));
    }

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds The list of vertex ids.
     * @param endTime   Include all changes made up until the point in time.
     * @param user      The user required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    Stream<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, User user);

    /**
     * Given a list of vertices, find all the edges that connect them.
     *
     * @param vertices       The list of vertices.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    @FutureDeprecation
    default Iterable<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> vertices, Authorizations authorizations) {
        return toIterable(findRelatedEdgeSummaryForVertices(vertices, authorizations.getUser()));
    }

    /**
     * Given a list of vertices, find all the edges that connect them.
     *
     * @param vertices The list of vertices.
     * @param user     The user required to load the edges.
     * @return Summary information about the related edges.
     */
    default Stream<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> vertices, User user) {
        List<RelatedEdge> results = new ArrayList<>();
        List<Vertex> verticesList = IterableUtils.toList(vertices);
        for (Vertex outVertex : verticesList) {
            outVertex.getEdgeInfos(Direction.OUT, user)
                .forEach(edgeInfo -> {
                    for (Vertex inVertex : verticesList) {
                        if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                            results.add(new RelatedEdgeImpl(edgeInfo.getEdgeId(), edgeInfo.getLabel(), outVertex.getId(), inVertex.getId()));
                        }
                    }
                });
        }
        return results.stream();
    }

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    @FutureDeprecation
    default Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Authorizations authorizations) {
        return toIterable(findRelatedEdgeSummary(vertexIds, authorizations.getUser()));
    }

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds The list of vertex ids.
     * @param user      The user required to load the edges.
     * @return Summary information about the related edges.
     */
    default Stream<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, User user) {
        return findRelatedEdgeSummary(vertexIds, null, user);
    }

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    @FutureDeprecation
    default Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        return toIterable(findRelatedEdgeSummary(vertexIds, endTime, authorizations.getUser()));
    }

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds The list of vertex ids.
     * @param endTime   Include all changes made up until the point in time.
     * @param user      The user required to load the edges.
     * @return Summary information about the related edges.
     */
    Stream<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, User user);

    /**
     * Permanently deletes an edge from the graph.
     *
     * @param edge           The edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#deleteElement()}
     */
    @Deprecated
    default void deleteEdge(Edge edge, Authorizations authorizations) {
        edge.prepareMutation()
            .deleteElement()
            .save(authorizations);
    }

    /**
     * Permanently deletes an edge from the graph. This method requires fetching the edge before deletion.
     *
     * @param edgeId         The edge id of the edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#deleteElement()}
     */
    @Deprecated
    default void deleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to delete with id: " + edgeId);
        deleteEdge(edge, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement()}
     */
    @Deprecated
    default void softDeleteEdge(Edge edge, Authorizations authorizations) {
        softDeleteEdge(edge, (Object) null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Object)}
     */
    @Deprecated
    default void softDeleteEdge(Edge edge, Object eventData, Authorizations authorizations) {
        softDeleteEdge(edge, null, eventData, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long, Object)}
     */
    @Deprecated
    default void softDeleteEdge(Edge edge, Long timestamp, Object eventData, Authorizations authorizations) {
        edge.prepareMutation()
            .softDeleteElement(timestamp, eventData)
            .save(authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long)}
     */
    @Deprecated
    default void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        softDeleteEdge(edge, timestamp, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement()}
     */
    @Deprecated
    default void softDeleteEdge(String edgeId, Authorizations authorizations) {
        softDeleteEdge(edgeId, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Object)}
     */
    @Deprecated
    default void softDeleteEdge(String edgeId, Object eventData, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, null, eventData, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long)}
     */
    @Deprecated
    default void softDeleteEdge(String edgeId, Long timestamp, Authorizations authorizations) {
        softDeleteEdge(edgeId, timestamp, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param eventData      Data to store with the soft delete
     * @param authorizations The authorizations required to delete the edge.
     * @deprecated Use {@link ElementMutation#softDeleteElement(Long, Object)}
     */
    @Deprecated
    default void softDeleteEdge(String edgeId, Long timestamp, Object eventData, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, timestamp, eventData, authorizations);
    }

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    @FutureDeprecation
    default GraphQuery query(String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryString, authorizations);
    }

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param queryString The string to search for in the text of an element. This will search all fields for the given text.
     * @param user        The user required to load the elements.
     * @return A query builder object.
     */
    default org.vertexium.search.GraphQuery query(String queryString, User user) {
        return getSearchIndex().queryGraph(this, queryString, user);
    }

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    @FutureDeprecation
    default GraphQuery query(Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, null, authorizations);
    }

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param user The user required to load the elements.
     * @return A query builder object.
     */
    default org.vertexium.search.GraphQuery query(User user) {
        return query((String) null, user);
    }

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    @FutureDeprecation
    default MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, queryString, authorizations);
    }

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds   The vertex ids to query.
     * @param queryString The string to search for in the text of an element. This will search all fields for the given text.
     * @param user        The user required to load the elements.
     * @return A query builder object.
     */
    default org.vertexium.search.MultiVertexQuery query(String[] vertexIds, String queryString, User user) {
        return getSearchIndex().queryGraph(this, vertexIds, queryString, user);
    }

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    @FutureDeprecation
    default MultiVertexQuery query(String[] vertexIds, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, null, authorizations);
    }

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds The vertex ids to query.
     * @param user      The user required to load the elements.
     * @return A query builder object.
     */
    default org.vertexium.search.MultiVertexQuery query(String[] vertexIds, User user) {
        return query(vertexIds, null, user);
    }

    /**
     * Returns true if this graph supports similar to text queries.
     */
    default boolean isQuerySimilarToTextSupported() {
        return getSearchIndex().isQuerySimilarToTextSupported();
    }

    /**
     * Creates a query builder object that finds all vertices similar to the given text for the specified fields.
     * This could be implemented similar to the ElasticSearch more like this query.
     *
     * @param fields         The fields to match against.
     * @param text           The text to find similar to.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    @FutureDeprecation
    default SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        return getSearchIndex().querySimilarTo(this, fields, text, authorizations);
    }

    /**
     * Creates a query builder object that finds all vertices similar to the given text for the specified fields.
     * This could be implemented similar to the ElasticSearch more like this query.
     *
     * @param fields The fields to match against.
     * @param text   The text to find similar to.
     * @param user   The user required to load the elements.
     * @return A query builder object.
     */
    default org.vertexium.search.SimilarToGraphQuery querySimilarTo(String[] fields, String text, User user) {
        return getSearchIndex().querySimilarTo(this, fields, text, user);
    }

    /**
     * Flushes any pending mutations to the graph.
     */
    default void flush() {
        if (getSearchIndex() != null) {
            getSearchIndex().flush(this);
        }
    }

    /**
     * This method will only flush the primary graph and not the search index
     */
    void flushGraph();

    SearchIndex getSearchIndex();

    /**
     * Cleans up or disconnects from the underlying storage.
     */
    default void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            getSearchIndex().shutdown();
        }
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param options        Find path options
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    @FutureDeprecation
    default Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations) {
        return toIterable(findPaths(options, authorizations.getUser()));
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param options Find path options
     * @param user    The user required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Stream<Path> findPaths(FindPathOptions options, User user);

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
    @FutureDeprecation
    default boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return isVisibilityValid(visibility, authorizations.getUser());
    }

    /**
     * Given an authorization is the visibility object valid.
     *
     * @param visibility The visibility you want to check.
     * @param user       The given user.
     * @return true if the visibility is valid given an authorization, else return false.
     */
    boolean isVisibilityValid(Visibility visibility, User user);

    /**
     * Reindex all vertices and edges.
     *
     * @param authorizations authorizations used to query for the data to reindex.
     */
    @Deprecated
    default void reindex(Authorizations authorizations) {
        for (Vertex vertex : getVertices(authorizations)) {
            getSearchIndex().addElement(this, vertex, authorizations.getUser());
        }
        for (Edge edge : getEdges(authorizations)) {
            getSearchIndex().addElement(this, edge, authorizations.getUser());
        }
    }

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
    default boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

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
    @Deprecated
    default SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }

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
     * @deprecated Use {@link ElementMutation#markElementHidden(Visibility)}
     */
    @Deprecated
    default void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        markVertexHidden(vertex, visibility, null, authorizations);
    }

    /**
     * Marks a vertex as hidden for a given visibility.
     *
     * @param vertex         The vertex to mark hidden.
     * @param visibility     The visibility string under which this vertex is hidden.
     *                       This visibility can be a superset of the vertex visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param eventData      Data to store with the hidden
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementHidden(Visibility, Object)}
     */
    @Deprecated
    default void markVertexHidden(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        vertex.prepareMutation()
            .markElementHidden(visibility, eventData)
            .save(authorizations);
    }

    /**
     * Marks a vertex as visible for a given visibility, effectively undoing markVertexHidden.
     *
     * @param vertex         The vertex to mark visible.
     * @param visibility     The visibility string under which this vertex is now visible.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementVisible(Visibility)}
     */
    @Deprecated
    default void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        markVertexVisible(vertex, visibility, null, authorizations);
    }

    /**
     * Marks a vertex as visible for a given visibility, effectively undoing markVertexHidden.
     *
     * @param vertex         The vertex to mark visible.
     * @param visibility     The visibility string under which this vertex is now visible.
     * @param eventData      Data to store with the visible
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementVisible(Visibility, Object)}
     */
    @Deprecated
    default void markVertexVisible(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        vertex.prepareMutation()
            .markElementVisible(visibility, eventData)
            .save(authorizations);
    }

    /**
     * Marks an edge as hidden for a given visibility.
     *
     * @param edge           The edge to mark hidden.
     * @param visibility     The visibility string under which this edge is hidden.
     *                       This visibility can be a superset of the edge visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementHidden(Visibility)}
     */
    @Deprecated
    default void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        markEdgeHidden(edge, visibility, null, authorizations);
    }

    /**
     * Marks an edge as hidden for a given visibility.
     *
     * @param edge           The edge to mark hidden.
     * @param visibility     The visibility string under which this edge is hidden.
     *                       This visibility can be a superset of the edge visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param eventData      Data to store with the hidden
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementHidden(Visibility, Object)}
     */
    @Deprecated
    default void markEdgeHidden(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        edge.prepareMutation()
            .markElementHidden(visibility, eventData)
            .save(authorizations);
    }

    /**
     * Marks an edge as visible for a given visibility, effectively undoing markEdgeHidden.
     *
     * @param edge           The edge to mark visible.
     * @param visibility     The visibility string under which this edge is now visible.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementVisible(Visibility)}
     */
    @Deprecated
    default void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        markEdgeVisible(edge, visibility, null, authorizations);
    }

    /**
     * Marks an edge as visible for a given visibility, effectively undoing markEdgeHidden.
     *
     * @param edge           The edge to mark visible.
     * @param visibility     The visibility string under which this edge is now visible.
     * @param eventData      Data to store with the visible
     * @param authorizations The authorizations used.
     * @deprecated Use {@link ElementMutation#markElementVisible(Visibility, Object)}
     */
    @Deprecated
    default void markEdgeVisible(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        edge.prepareMutation()
            .markElementVisible(visibility, eventData)
            .save(authorizations);
    }

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    @FutureDeprecation
    Authorizations createAuthorizations(String... auths);

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    @FutureDeprecation
    default Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[0]));
    }

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    @FutureDeprecation
    default Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations) {
        Set<String> newAuths = new HashSet<>();
        Collections.addAll(newAuths, auths.getAuthorizations());
        Collections.addAll(newAuths, additionalAuthorizations);
        return createAuthorizations(newAuths);
    }

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    @FutureDeprecation
    default Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations) {
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[0]));
    }

    /**
     * Gets the number of times a property with a given value occurs on vertices
     *
     * @param propertyName   The name of the property to find
     * @param authorizations The authorizations to use to find the property
     * @return The results
     * @deprecated Use {@link org.vertexium.query.Query#addAggregation(Aggregation)}
     */
    @Deprecated
    default Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        Map<Object, Long> countsByValue = new HashMap<>();
        for (Vertex v : getVertices(authorizations)) {
            for (Property p : v.getProperties()) {
                if (propertyName.equals(p.getName())) {
                    Object mapKey = p.getValue();
                    if (mapKey instanceof String) {
                        mapKey = ((String) mapKey).toLowerCase();
                    }
                    Long currentValue = countsByValue.get(mapKey);
                    if (currentValue == null) {
                        countsByValue.put(mapKey, 1L);
                    } else {
                        countsByValue.put(mapKey, currentValue + 1);
                    }
                }
            }
        }
        return countsByValue;
    }

    /**
     * Gets a count of the number of vertices in the system.
     *
     * @deprecated Use {@link #query(User)}.{@link Query#vertices()}.{@link QueryResultsIterable#getTotalHits()}
     */
    @Deprecated
    default long getVertexCount(Authorizations authorizations) {
        return count(getVertices(authorizations));
    }

    /**
     * Gets a count of the number of edges in the system.
     *
     * @deprecated Use {@link #query(User)}.{@link Query#vertices()}.{@link QueryResultsIterable#getTotalHits()}
     */
    @Deprecated
    default long getEdgeCount(Authorizations authorizations) {
        return count(getEdges(authorizations));
    }

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
    default DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                savePropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
    }

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
    @FutureDeprecation
    default Iterable<Element> saveElementMutations(
        Iterable<ElementMutation<? extends Element>> mutations,
        Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();
        for (ElementMutation<? extends Element> m : mutations) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            Element element = m.save(authorizations);
            elements.add(element);
        }
        for (ElementMutation<? extends Element> m : mutations) {
            if (m.getIndexHint() == IndexHint.INDEX) {
                getSearchIndex().addElementExtendedData(
                    this,
                    m,
                    m.getExtendedData(),
                    m.getAdditionalExtendedDataVisibilities(),
                    m.getAdditionalExtendedDataVisibilityDeletes(),
                    authorizations
                );
            }
        }
        return elements;
    }

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
    @FutureDeprecation
    default Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, Authorizations authorizations) {
        return getExtendedData(ids, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param ids  The ids of the rows to get.
     * @param user The user used to get the rows
     * @return Rows
     */
    default Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, User user) {
        return getExtendedData(ids, getDefaultFetchHints(), user);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param ids            The ids of the rows to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    @FutureDeprecation
    default Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getExtendedData(ids, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param ids  The ids of the rows to get.
     * @param user The user used to get the rows
     * @return Rows
     */
    Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, User user);

    /**
     * Gets the specified extended data row.
     *
     * @param id             The id of the row to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    @FutureDeprecation
    default ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations) {
        return getExtendedData(id, authorizations.getUser());
    }

    /**
     * Gets the specified extended data row.
     *
     * @param id   The id of the row to get.
     * @param user The user used to get the rows
     * @return Rows
     */
    default ExtendedDataRow getExtendedData(ExtendedDataRowId id, User user) {
        List<ExtendedDataRow> rows = getExtendedData(Lists.newArrayList(id), user).collect(Collectors.toList());
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new VertexiumException("Expected 0 or 1 rows found " + rows.size());
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    @FutureDeprecation
    default Iterable<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        Authorizations authorizations
    ) {
        return getExtendedData(elementType, elementId, tableName, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType The type of element to get the rows from
     * @param elementId   The element id to get the rows from
     * @param tableName   The name of the table within the element to get the rows from
     * @param user        The user used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        User user
    ) {
        return toIterable(getExtendedData(elementType, elementId, tableName, getDefaultFetchHints(), user));
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param fetchHints     Fetch hints to filter extended data
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    @FutureDeprecation
    default Iterable<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        return toIterable(getExtendedData(elementType, elementId, tableName, fetchHints, authorizations.getUser()));
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType The type of element to get the rows from
     * @param elementId   The element id to get the rows from
     * @param tableName   The name of the table within the element to get the rows from
     * @param user        The user used to get the rows
     * @return Rows
     */
    Stream<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    );

    /**
     * Gets extended data rows from the graph in the given range.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementIdRange The range of element ids to get extended data rows for.
     * @param authorizations The authorizations required to load the vertex.
     * @return The extended data rows for the element ids in the range.
     */
    @FutureDeprecation
    default Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, Authorizations authorizations) {
        return toIterable(getExtendedDataInRange(elementType, elementIdRange, authorizations.getUser()));
    }

    /**
     * Gets extended data rows from the graph in the given range.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementIdRange The range of element ids to get extended data rows for.
     * @param user           The user required to load the vertex.
     * @return The extended data rows for the element ids in the range.
     */
    Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, User user);

    /**
     * Gets a list of historical events.
     *
     * @param elementIds     Iterable of element ids to get events for
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    @FutureDeprecation
    default Stream<HistoricalEvent> getHistoricalEvents(Iterable<ElementId> elementIds, Authorizations authorizations) {
        return getHistoricalEvents(elementIds, getDefaultHistoricalEventsFetchHints(), authorizations);
    }

    /**
     * Gets a list of historical events.
     *
     * @param elementIds Iterable of element ids to get events for
     * @param user       The user required to load the events
     * @return An iterable of historic events
     */
    default Stream<HistoricalEvent> getHistoricalEvents(Iterable<ElementId> elementIds, User user) {
        return getHistoricalEvents(elementIds, getDefaultHistoricalEventsFetchHints(), user);
    }

    /**
     * Gets a list of historical events.
     *
     * @param elementIds     Iterable of element ids to get events for
     * @param fetchHints     Fetch hints to filter historical events
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    @FutureDeprecation
    default Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        return getHistoricalEvents(elementIds, null, fetchHints, authorizations);
    }

    /**
     * Gets a list of historical events.
     *
     * @param elementIds Iterable of element ids to get events for
     * @param fetchHints Fetch hints to filter historical events
     * @param user       The user required to load the events
     * @return An iterable of historic events
     */
    default Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        return getHistoricalEvents(elementIds, null, fetchHints, user);
    }

    /**
     * Gets a list of historical events.
     *
     * @param elementIds     Iterable of element ids to get events for
     * @param after          Find events after the given id
     * @param fetchHints     Fetch hints to filter historical events
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    @FutureDeprecation
    default Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        return getHistoricalEvents(elementIds, after, fetchHints, authorizations.getUser());
    }

    /**
     * Gets a list of historical events.
     *
     * @param elementIds Iterable of element ids to get events for
     * @param after      Find events after the given id
     * @param fetchHints Fetch hints to filter historical events
     * @param user       The user required to load the events
     * @return An iterable of historic events
     */
    default Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return fetchHints.applyToResults(stream(elementIds)
            .flatMap(elementId -> {
                Element element = getElement(elementId, elementFetchHints, user);
                if (element == null) {
                    throw new VertexiumException("Could not find: " + elementId);
                }
                return element.getHistoricalEvents(after, fetchHints, user);
            }), after);
    }

    /**
     * Deletes an extended data row
     *
     * @deprecated Use {@link ElementMutation#deleteExtendedDataRow(String, String)}
     */
    @Deprecated
    default void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations) {
        deleteExtendedDataRow(id, authorizations.getUser());
    }

    /**
     * Deletes an extended data row
     *
     * @deprecated Use {@link ElementMutation#deleteExtendedDataRow(String, String)}
     */
    @Deprecated
    default void deleteExtendedDataRow(ExtendedDataRowId id, User user) {
        ElementId elementId = new ElementId(id.getElementType(), id.getElementId());
        Element element = getElement(elementId, user);
        element.prepareMutation()
            .deleteExtendedDataRow(id.getTableName(), id.getRowId())
            .save(user);
    }

    /**
     * The default fetch hints to use if none are provided
     */
    default FetchHints getDefaultFetchHints() {
        return FetchHints.ALL;
    }

    /**
     * The default fetch hints to use if none are provided
     */
    default HistoricalEventsFetchHints getDefaultHistoricalEventsFetchHints() {
        return HistoricalEventsFetchHints.ALL;
    }

    /**
     * Visits all elements on the graph
     */
    @Deprecated
    default void visitElements(GraphVisitor graphVisitor, Authorizations authorizations) {
        visitVertices(graphVisitor, authorizations);
        visitEdges(graphVisitor, authorizations);
    }

    /**
     * Visits all vertices on the graph
     */
    @Deprecated
    default void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getVertices(authorizations), graphVisitor);
    }

    /**
     * Visits all edges on the graph
     */
    @Deprecated
    default void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getEdges(authorizations), graphVisitor);
    }

    /**
     * Visits elements using the supplied elements and visitor
     */
    @Deprecated
    default void visit(Iterable<? extends Element> elements, GraphVisitor visitor) {
        for (Element element : elements) {
            visitor.visitElement(element);
            if (element instanceof Vertex) {
                visitor.visitVertex((Vertex) element);
            } else if (element instanceof Edge) {
                visitor.visitEdge((Edge) element);
            } else {
                throw new VertexiumException("Invalid element type to visit: " + element.getClass().getName());
            }

            for (Property property : element.getProperties()) {
                visitor.visitProperty(element, property);
            }

            for (String tableName : element.getExtendedDataTableNames()) {
                for (ExtendedDataRow extendedDataRow : element.getExtendedData(tableName)) {
                    visitor.visitExtendedDataRow(element, tableName, extendedDataRow);
                    for (Property property : extendedDataRow.getProperties()) {
                        visitor.visitProperty(element, tableName, extendedDataRow, property);
                    }
                }
            }
        }
    }
}
