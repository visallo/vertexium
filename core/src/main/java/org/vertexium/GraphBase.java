package org.vertexium;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.vertexium.event.GraphEvent;
import org.vertexium.event.GraphEventListener;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.id.IdGenerator;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.util.*;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public abstract class GraphBase implements Graph {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphBase.class);
    protected static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Graph.class);
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    private final List<GraphEventListener> graphEventListeners = new ArrayList<>();
    private Map<String, PropertyDefinition> propertyDefinitionCache = new ConcurrentHashMap<>();
    private final boolean strictTyping;

    protected GraphBase(boolean strictTyping) {
        this.strictTyping = strictTyping;
    }

    @Override
    public Vertex addVertex(Visibility visibility, Authorizations authorizations) {
        return prepareVertex(visibility).save(authorizations);
    }

    @Override
    public Vertex addVertex(String vertexId, Visibility visibility, Authorizations authorizations) {
        return prepareVertex(vertexId, visibility).save(authorizations);
    }

    @Override
    public Iterable<Vertex> addVertices(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations) {
        List<Vertex> addedVertices = new ArrayList<>();
        for (ElementBuilder<Vertex> vertexBuilder : vertices) {
            addedVertices.add(vertexBuilder.save(authorizations));
        }
        return addedVertices;
    }

    @Override
    public VertexBuilder prepareVertex(Visibility visibility) {
        return prepareVertex(getIdGenerator().nextId(), null, visibility);
    }

    @Override
    public abstract VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility);

    @Override
    public VertexBuilder prepareVertex(Long timestamp, Visibility visibility) {
        return prepareVertex(getIdGenerator().nextId(), timestamp, visibility);
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        return prepareVertex(vertexId, null, visibility);
    }

    @Override
    public boolean doesVertexExist(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, FetchHints.NONE, authorizations) != null;
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getVertex(vertexId, fetchHints, null, authorizations);
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        for (Vertex vertex : getVertices(fetchHints, endTime, authorizations)) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public Vertex getVertex(String vertexId, Authorizations authorizations) throws VertexiumException {
        return getVertex(vertexId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(final String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return v.getId().startsWith(vertexIdPrefix);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(Range idRange, Authorizations authorizations) {
        return getVerticesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesInRange(idRange, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(final Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return idRange.isInRange(v.getId());
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, FetchHints fetchHints, final Authorizations authorizations) {
        return getVertices(ids, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        return new LookAheadIterable<String, Vertex>() {
            @Override
            protected boolean isIncluded(String src, Vertex vertex) {
                return vertex != null;
            }

            @Override
            protected Vertex convert(String id) {
                return getVertex(id, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return Sets.newHashSet(ids).iterator();
            }
        };
    }

    @Override
    public Map<String, Boolean> doVerticesExist(Iterable<String> ids, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Vertex vertex : getVertices(ids, FetchHints.NONE, authorizations)) {
            results.put(vertex.getId(), true);
        }
        return results;
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, final Authorizations authorizations) {
        return getVertices(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        final List<String> vertexIds = IterableUtils.toList(ids);
        List<Vertex> vertices = IterableUtils.toList(getVertices(vertexIds, authorizations));
        vertices.sort((v1, v2) -> {
            Integer i1 = vertexIds.indexOf(v1.getId());
            Integer i2 = vertexIds.indexOf(v2.getId());
            return i1.compareTo(i2);
        });
        return vertices;
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations) {
        return getVerticesInOrder(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) throws VertexiumException {
        return getVertices(getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(fetchHints, null, authorizations);
    }

    @Override
    public abstract Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertex, inVertex, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertexId, inVertexId, label, visibility);
    }

    @Override
    public EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility);
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

    @Override
    public abstract EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

    @Override
    public abstract EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    @Override
    public boolean doesEdgeExist(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, FetchHints.NONE, authorizations) != null;
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Authorizations authorizations) {
        return getEdge(edgeId, fetchHints, null, authorizations);
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        for (Edge edge : getEdges(fetchHints, endTime, authorizations)) {
            if (edge.getId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public Edge getEdge(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Map<String, Boolean> doEdgesExist(Iterable<String> ids, Authorizations authorizations) {
        return doEdgesExist(ids, null, authorizations);
    }

    @Override
    public Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Edge edge : getEdges(ids, FetchHints.NONE, endTime, authorizations)) {
            results.put(edge.getId(), true);
        }
        return results;
    }

    @Override
    public void deleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to delete with id: " + vertexId);
        deleteVertex(vertex, authorizations);
    }

    @Override
    public void deleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to delete with id: " + edgeId);
        deleteEdge(edge, authorizations);
    }

    @Override
    public void softDeleteVertex(String vertexId, Object eventData, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, null, eventData, authorizations);
    }

    @Override
    public void softDeleteVertex(String vertexId, Long timestamp, Object eventData, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, timestamp, eventData, authorizations);
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Object eventData, Authorizations authorizations) {
        softDeleteVertex(vertex, null, eventData, authorizations);
    }

    @Override
    public abstract void softDeleteVertex(Vertex vertex, Long timestamp, Object eventData, Authorizations authorizations);

    @Override
    public void softDeleteEdge(String edgeId, Object eventData, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, null, eventData, authorizations);
    }

    @Override
    public void softDeleteEdge(String edgeId, Long timestamp, Object eventData, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, timestamp, eventData, authorizations);
    }

    @Override
    public void softDeleteEdge(Edge edge, Object eventData, Authorizations authorizations) {
        softDeleteEdge(edge, null, eventData, authorizations);
    }

    @Override
    public abstract void softDeleteEdge(Edge edge, Long timestamp, Object eventData, Authorizations authorizations);

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(
        Iterable<String> edgeIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Edge> edges = new FilterIterable<Edge>(getEdges(edgeIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (edge.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(edge, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Edge, String>(edges) {
            @Override
            protected String convert(Edge edge) {
                return edge.getId();
            }
        };
    }

    private boolean isIncludedByAuthorizations(Element element, EnumSet<ElementFilter> filters, String authorizationToMatch) {
        if (filters.contains(ElementFilter.PROPERTY) || filters.contains(ElementFilter.PROPERTY_METADATA)) {
            for (Property property : element.getProperties()) {
                if (filters.contains(ElementFilter.PROPERTY)) {
                    if (property.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                if (filters.contains(ElementFilter.PROPERTY_METADATA)) {
                    for (Metadata.Entry entry : property.getMetadata().entrySet()) {
                        if (entry.getVisibility().hasAuthorization(authorizationToMatch)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(
        Iterable<String> vertexIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Vertex> vertices = new FilterIterable<Vertex>(getVertices(vertexIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex vertex) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (vertex.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(vertex, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex vertex) {
                return vertex.getId();
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final FetchHints fetchHints, final Authorizations authorizations) {
        return getEdges(ids, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        return new LookAheadIterable<String, Edge>() {
            @Override
            protected boolean isIncluded(String src, Edge edge) {
                return edge != null;
            }

            @Override
            protected Edge convert(String id) {
                return getEdge(id, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return Sets.newHashSet(ids).iterator();
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final Authorizations authorizations) {
        return getEdges(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdges(getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(fetchHints, null, authorizations);
    }

    @Override
    public abstract Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public Iterable<Edge> getEdgesInRange(Range idRange, Authorizations authorizations) {
        return getEdgesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgesInRange(idRange, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdgesInRange(final Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Edge> edges = getEdges(fetchHints, endTime, authorizations);
        return new FilterIterable<Edge>(edges) {
            @Override
            protected boolean isIncluded(Edge e) {
                return idRange.isInRange(e.getId());
            }
        };
    }

    @Override
    public Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        FetchHints fetchHints = FetchHints.EDGE_REFS;
        Vertex sourceVertex = getVertex(options.getSourceVertexId(), fetchHints, authorizations);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getSourceVertexId());
        }
        Vertex destVertex = getVertex(options.getDestVertexId(), fetchHints, authorizations);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getDestVertexId());
        }

        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        Set<String> seenVertices = new HashSet<>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() == 2) {
            findPathsSetIntersection(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                progressCallback,
                authorizations
            );
        } else {
            findPathsRecursive(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                options.getMaxHops(),
                seenVertices,
                startPath,
                progressCallback,
                authorizations
            );
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths;
    }

    protected void findPathsSetIntersection(FindPathOptions options, List<Path> foundPaths, Vertex sourceVertex, Vertex destVertex, ProgressCallback progressCallback, Authorizations authorizations) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = filterFindPathEdgeInfo(options, sourceVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), authorizations));
        Map<String, Boolean> sourceVerticesExist = doVerticesExist(sourceVertexConnectedVertexIds, authorizations);
        sourceVertexConnectedVertexIds = stream(sourceVerticesExist.keySet())
            .filter(key -> sourceVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = filterFindPathEdgeInfo(options, destVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), authorizations));
        Map<String, Boolean> destVerticesExist = doVerticesExist(destVertexConnectedVertexIds, authorizations);
        destVertexConnectedVertexIds = stream(destVerticesExist.keySet())
            .filter(key -> destVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private Set<String> filterFindPathEdgeInfo(FindPathOptions options, Iterable<EdgeInfo> edgeInfos) {
        return stream(edgeInfos)
            .filter(edgeInfo -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgeInfo.getLabel());
                }
                return true;
            })
            .map(EdgeInfo::getVertexId)
            .collect(Collectors.toSet());
    }

    private Iterable<Vertex> filterFindPathEdgePairs(FindPathOptions options, Iterable<EdgeVertexPair> edgeVertexPairs) {
        return stream(edgeVertexPairs)
            .filter(edgePair -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgePair.getEdge().getLabel());
                }
                return true;
            })
            .map(EdgeVertexPair::getVertex)
            .collect(Collectors.toList());
    }

    protected void findPathsRecursive(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        int hops,
        Set<String> seenVertices,
        Path currentPath,
        ProgressCallback progressCallback,
        Authorizations authorizations
    ) {
        // if this is our first source vertex report progress back to the progress callback
        boolean firstLevelRecursion = hops == options.getMaxHops();

        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }

        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Iterable<Vertex> vertices = filterFindPathEdgePairs(options, sourceVertex.getEdgeVertexPairs(Direction.BOTH, options.getLabels(), authorizations));
            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertices = IterableUtils.toList(vertices);
                vertexCount = ((List<Vertex>) vertices).size();
            }
            int i = 0;
            for (Vertex child : vertices) {
                if (firstLevelRecursion) {
                    // this will never get to 100% since i starts at 0. which is good. 100% signifies done and we still have work to do.
                    double progressPercent = (double) i / (double) vertexCount;
                    progressCallback.progress(progressPercent, ProgressCallback.Step.SEARCHING_EDGES, i + 1, vertexCount);
                }
                if (!seenVertices.contains(child.getId())) {
                    findPathsRecursive(options, foundPaths, child, destVertex, hops - 1, seenVertices, new Path(currentPath, child.getId()), progressCallback, authorizations);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, null, authorizations);
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeIdsForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeSummary(vertexIds, null, authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeSummaryForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
        List<RelatedEdge> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            Iterable<EdgeInfo> edgeInfos = outVertex.getEdgeInfos(Direction.OUT, authorizations);
            for (EdgeInfo edgeInfo : edgeInfos) {
                for (Vertex inVertex : vertices) {
                    if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                        results.add(new RelatedEdgeImpl(edgeInfo.getEdgeId(), edgeInfo.getLabel(), outVertex.getId(), inVertex.getId()));
                    }
                }
            }
        }
        return results;
    }

    @Override
    public Iterable<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
        List<String> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            if (outVertex == null) {
                throw new VertexiumException("verticesIterable cannot have null values");
            }
            Iterable<EdgeInfo> edgeInfos = outVertex.getEdgeInfos(Direction.OUT, authorizations);
            for (EdgeInfo edgeInfo : edgeInfos) {
                for (Vertex inVertex : vertices) {
                    if (edgeInfo.getVertexId() == null) { // This check is for legacy data. null EdgeInfo.vertexIds are no longer permitted
                        continue;
                    }
                    if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                        results.add(edgeInfo.getEdgeId());
                    }
                }
            }
        }
        return results;
    }

    protected abstract GraphMetadataStore getGraphMetadataStore();

    @Override
    public final Iterable<GraphMetadataEntry> getMetadata() {
        return getGraphMetadataStore().getMetadata();
    }

    @Override
    public final void setMetadata(String key, Object value) {
        getGraphMetadataStore().setMetadata(key, value);
    }

    @Override
    public final Object getMetadata(String key) {
        return getGraphMetadataStore().getMetadata(key);
    }

    @Override
    public final Iterable<GraphMetadataEntry> getMetadataWithPrefix(String prefix) {
        return getGraphMetadataStore().getMetadataWithPrefix(prefix);
    }

    @Override
    public abstract GraphQuery query(Authorizations authorizations);

    @Override
    public abstract GraphQuery query(String queryString, Authorizations authorizations);

    @Override
    public abstract void reindex(Authorizations authorizations);

    @Override
    public abstract void flush();

    @Override
    public abstract void shutdown();

    @Override
    public abstract void drop();

    @Override
    public abstract boolean isFieldBoostSupported();

    @Override
    public abstract SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    @Override
    public abstract FetchHints getDefaultFetchHints();

    @Override
    public void addGraphEventListener(GraphEventListener graphEventListener) {
        this.graphEventListeners.add(graphEventListener);
    }

    protected boolean hasEventListeners() {
        return this.graphEventListeners.size() > 0;
    }

    protected void fireGraphEvent(GraphEvent graphEvent) {
        for (GraphEventListener graphEventListener : this.graphEventListeners) {
            graphEventListener.onGraphEvent(graphEvent);
        }
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return false;
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        throw new VertexiumException("querySimilarTo not supported");
    }

    @Override
    public Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[auths.size()]));
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations) {
        Set<String> newAuths = new HashSet<>();
        Collections.addAll(newAuths, auths.getAuthorizations());
        Collections.addAll(newAuths, additionalAuthorizations);
        return createAuthorizations(newAuths);
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations) {
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[additionalAuthorizations.size()]));
    }

    @Override
    @Deprecated
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
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

    @Override
    public long getVertexCount(Authorizations authorizations) {
        return count(getVertices(authorizations));
    }

    @Override
    public long getEdgeCount(Authorizations authorizations) {
        return count(getEdges(authorizations));
    }

    @Override
    public abstract void deleteVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public abstract void deleteEdge(Edge edge, Authorizations authorizations);

    @Override
    public abstract void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations);

    @Override
    public abstract MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations);

    @Override
    public abstract MultiVertexQuery query(String[] vertexIds, Authorizations authorizations);

    @Override
    public abstract IdGenerator getIdGenerator();

    @Override
    public abstract boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void truncate();

    @Override
    public abstract void markVertexHidden(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations);

    @Override
    public abstract void markVertexVisible(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations);

    @Override
    public abstract void markEdgeHidden(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations);

    @Override
    public abstract void markEdgeVisible(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations);

    @Override
    public abstract Authorizations createAuthorizations(String... auths);

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                savePropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
    }

    protected void addToPropertyDefinitionCache(PropertyDefinition propertyDefinition) {
        propertyDefinitionCache.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    protected void invalidatePropertyDefinition(String propertyName) {
        PropertyDefinition def = (PropertyDefinition) getMetadata(getPropertyDefinitionKey(propertyName));
        if (def == null) {
            propertyDefinitionCache.remove(propertyName);
        } else if (def != null) {
            addToPropertyDefinitionCache(def);
        }
    }

    public void savePropertyDefinition(PropertyDefinition propertyDefinition) {
        addToPropertyDefinitionCache(propertyDefinition);
        setMetadata(getPropertyDefinitionKey(propertyDefinition.getPropertyName()), propertyDefinition);
    }

    private String getPropertyDefinitionKey(String propertyName) {
        return METADATA_DEFINE_PROPERTY_PREFIX + propertyName;
    }

    @Override
    public PropertyDefinition getPropertyDefinition(String propertyName) {
        return propertyDefinitionCache.getOrDefault(propertyName, null);
    }

    @Override
    public Collection<PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitionCache.values();
    }

    @Override
    public boolean isPropertyDefined(String propertyName) {
        return propertyDefinitionCache.containsKey(propertyName);
    }

    public void ensurePropertyDefined(String name, Object value) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(name);
        if (propertyDefinition != null) {
            return;
        }
        Class<?> valueClass = getValueType(value);
        if (strictTyping) {
            throw new VertexiumTypeException(name, valueClass);
        }
        LOGGER.warn("creating default property definition because a previous definition could not be found for property \"" + name + "\" of type " + valueClass);
        propertyDefinition = new PropertyDefinition(name, valueClass, TextIndexHint.ALL);
        savePropertyDefinition(propertyDefinition);
    }

    protected Class<?> getValueType(Object value) {
        Class<?> valueClass = value.getClass();
        if (value instanceof StreamingPropertyValue) {
            valueClass = ((StreamingPropertyValue) value).getValueType();
        } else if (value instanceof StreamingPropertyValueRef) {
            valueClass = ((StreamingPropertyValueRef) value).getValueType();
        }
        return valueClass;
    }

    @Override
    public Iterable<Element> saveElementMutations(
        Iterable<ElementMutation<? extends Element>> mutations,
        Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();
        for (ElementMutation m : mutations) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            Element element = m.save(authorizations);
            elements.add(element);
        }
        return elements;
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> idsIterable, FetchHints fetchHints, Authorizations authorizations) {
        Set<ExtendedDataRowId> ids = Sets.newHashSet(idsIterable);
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                return ids.contains(row.getId());
            }
        };
    }

    @Override
    public ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations) {
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new VertexiumException("Expected 0 or 1 rows found " + rows.size());
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        if ((elementType == null && (elementId != null || tableName != null))
            || (elementType != null && elementId == null && tableName != null)) {
            throw new VertexiumException("Cannot create partial key with missing inner value");
        }

        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                return (elementType == null || elementType.equals(rowId.getElementType()))
                    && (elementId == null || elementId.equals(rowId.getElementId()))
                    && (tableName == null || tableName.equals(rowId.getTableName()));
            }
        };
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, Authorizations authorizations) {
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(FetchHints.ALL, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                return elementType.equals(rowId.getElementType())
                    && elementIdRange.isInRange(rowId.getElementId());
            }
        };
    }

    protected Iterable<ExtendedDataRow> getAllExtendedData(FetchHints fetchHints, Authorizations authorizations) {
        JoinIterable<Element> allElements = new JoinIterable<>(getVertices(fetchHints, authorizations), getEdges(fetchHints, authorizations));
        return new SelectManyIterable<Element, ExtendedDataRow>(allElements) {
            @Override
            protected Iterable<? extends ExtendedDataRow> getIterable(Element element) {
                return new SelectManyIterable<String, ExtendedDataRow>(element.getExtendedDataTableNames()) {
                    @Override
                    protected Iterable<? extends ExtendedDataRow> getIterable(String tableName) {
                        return element.getExtendedData(tableName);
                    }
                };
            }
        };
    }

    protected void deleteAllExtendedDataForElement(Element element, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludeExtendedDataTableNames() || element.getExtendedDataTableNames().size() <= 0) {
            return;
        }

        for (ExtendedDataRow row : getExtendedData(ElementType.getTypeFromElement(element), element.getId(), null, authorizations)) {
            deleteExtendedDataRow(row.getId(), authorizations);
        }
    }

    @Override
    public void visitElements(GraphVisitor graphVisitor, Authorizations authorizations) {
        visitVertices(graphVisitor, authorizations);
        visitEdges(graphVisitor, authorizations);
    }

    @Override
    public void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getVertices(authorizations), graphVisitor);
    }

    @Override
    public void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getEdges(authorizations), graphVisitor);
    }

    @Override
    public void visit(Iterable<? extends Element> elements, GraphVisitor visitor) {
        int i = 0;
        for (Element element : elements) {
            if (i % 1000000 == 0) {
                LOGGER.debug("checking: %s", element.getId());
            }

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

            i++;
        }
    }

    @Override
    public Element getElement(ElementId elementId, Authorizations authorizations) {
        return getElement(elementId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return fetchHints.applyToResults(stream(elementIds)
            .flatMap(elementId -> {
                Element element = getElement(elementId, elementFetchHints, authorizations);
                if (element == null) {
                    throw new VertexiumException("Could not find: " + elementId);
                }
                return element.getHistoricalEvents(after, fetchHints, authorizations);
            }), after);
    }
}
