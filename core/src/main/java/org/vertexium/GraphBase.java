package org.vertexium;

import org.vertexium.event.GraphEvent;
import org.vertexium.event.GraphEventListener;
import org.vertexium.path.PathFindingAlgorithm;
import org.vertexium.path.RecursivePathFindingAlgorithm;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.util.LookAheadIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.util.IterableUtils;

import java.util.*;

public abstract class GraphBase implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphBase.class);
    private final PathFindingAlgorithm pathFindingAlgorithm = new RecursivePathFindingAlgorithm();
    private final List<GraphEventListener> graphEventListeners = new ArrayList<>();

    protected GraphBase() {

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
        return prepareVertex(getIdGenerator().nextId(), visibility);
    }

    @Override
    public boolean doesVertexExist(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, FetchHint.NONE, authorizations) != null;
    }

    @Override
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        LOGGER.warn("Performing scan of all vertices! Override getVertex.");
        for (Vertex vertex : getVertices(fetchHints, authorizations)) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public Vertex getVertex(String vertexId, Authorizations authorizations) throws VertexiumException {
        return getVertex(vertexId, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        LOGGER.warn("Getting each vertex one by one! Override getVertices(java.lang.Iterable<java.lang.String>, Authorizations)");
        return new LookAheadIterable<String, Vertex>() {
            @Override
            protected boolean isIncluded(String src, Vertex vertex) {
                return vertex != null;
            }

            @Override
            protected Vertex convert(String id) {
                return getVertex(id, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return ids.iterator();
            }
        };
    }

    @Override
    public Map<String, Boolean> doVerticesExist(List<String> ids, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Vertex vertex : getVertices(ids, FetchHint.NONE, authorizations)) {
            results.put(vertex.getId(), true);
        }
        return results;
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, final Authorizations authorizations) {
        return getVertices(ids, FetchHint.ALL, authorizations);
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        final List<String> vertexIds = IterableUtils.toList(ids);
        List<Vertex> vertices = IterableUtils.toList(getVertices(vertexIds, authorizations));
        Collections.sort(vertices, new Comparator<Vertex>() {
            @Override
            public int compare(Vertex v1, Vertex v2) {
                Integer i1 = vertexIds.indexOf(v1.getId());
                Integer i2 = vertexIds.indexOf(v2.getId());
                return i1.compareTo(i2);
            }
        });
        return vertices;
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations) {
        return getVerticesInOrder(ids, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) throws VertexiumException {
        return getVertices(FetchHint.ALL, authorizations);
    }

    @Override
    public abstract Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    @Override
    public abstract void removeVertex(Vertex vertex, Authorizations authorizations);

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
    public boolean doesEdgeExist(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, FetchHint.NONE, authorizations) != null;
    }

    @Override
    public Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        LOGGER.warn("Performing scan of all edges! Override getEdge.");
        for (Edge edge : getEdges(fetchHints, authorizations)) {
            if (edge.getId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public Edge getEdge(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, FetchHint.ALL, authorizations);
    }

    @Override
    public Map<String, Boolean> doEdgesExist(List<String> ids, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Edge edge : getEdges(ids, FetchHint.NONE, authorizations)) {
            results.put(edge.getId(), true);
        }
        return results;
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        LOGGER.warn("Getting each edge one by one! Override getEdges(java.lang.Iterable<java.lang.String>, Authorizations)");
        return new LookAheadIterable<String, Edge>() {
            @Override
            protected boolean isIncluded(String src, Edge edge) {
                return edge != null;
            }

            @Override
            protected Edge convert(String id) {
                return getEdge(id, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return ids.iterator();
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final Authorizations authorizations) {
        return getEdges(ids, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdges(FetchHint.ALL, authorizations);
    }

    @Override
    public abstract Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Authorizations authorizations);

    @Override
    public abstract void removeEdge(Edge edge, Authorizations authorizations);

    @Override
    public Iterable<Path> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, Authorizations authorizations) {
        ProgressCallback progressCallback = new ProgressCallback() {
            @Override
            public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                LOGGER.debug(String.format("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount)));
            }
        };
        return findPaths(sourceVertex, destVertex, maxHops, progressCallback, authorizations);
    }

    @Override
    public Iterable<Path> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        return pathFindingAlgorithm.findPaths(this, sourceVertex, destVertex, maxHops, progressCallback, authorizations);
    }

    @Override
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        EnumSet<FetchHint> fetchHints = FetchHint.EDGE_REFS;
        Vertex sourceVertex = getVertex(sourceVertexId, fetchHints, authorizations);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + sourceVertexId);
        }
        Vertex destVertex = getVertex(destVertexId, fetchHints, authorizations);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + destVertexId);
        }
        return findPaths(sourceVertex, destVertex, maxHops, progressCallback, authorizations);
    }

    @Override
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, Authorizations authorizations) {
        EnumSet<FetchHint> fetchHints = FetchHint.EDGE_REFS;
        Vertex sourceVertex = getVertex(sourceVertexId, fetchHints, authorizations);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + sourceVertexId);
        }
        Vertex destVertex = getVertex(destVertexId, fetchHints, authorizations);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + destVertexId);
        }
        return findPaths(sourceVertex, destVertex, maxHops, authorizations);
    }

    @Override
    public Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations) {
        Set<String> results = new HashSet<>();
        List<Vertex> vertices = IterableUtils.toList(getVertices(vertexIds, authorizations));

        // since we are checking bi-directional edges we should only have to check v1->v2 and not v2->v1
        Map<String, String> checkedCombinations = new HashMap<>();

        for (Vertex sourceVertex : vertices) {
            for (Vertex destVertex : vertices) {
                if (checkedCombinations.containsKey(sourceVertex.getId() + destVertex.getId())) {
                    continue;
                }
                Iterable<String> edgeIds = sourceVertex.getEdgeIds(destVertex, Direction.BOTH, authorizations);
                for (String edgeId : edgeIds) {
                    results.add(edgeId);
                }
                checkedCombinations.put(sourceVertex.getId() + destVertex.getId(), "");
                checkedCombinations.put(destVertex.getId() + sourceVertex.getId(), "");
            }
        }
        return results;
    }

    @Override
    public void removeEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        if (edge == null) {
            throw new IllegalArgumentException("Could not find edge with id: " + edgeId);
        }
        removeEdge(edge, authorizations);
    }

    @Override
    public abstract Iterable<GraphMetadataEntry> getMetadata();

    @Override
    public Object getMetadata(String key) {
        for (GraphMetadataEntry e : getMetadata()) {
            if (e.getKey().equals(key)) {
                return e.getValue();
            }
        }
        return null;
    }

    @Override
    public abstract void setMetadata(String key, Object value);

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
    public abstract DefinePropertyBuilder defineProperty(String propertyName);

    @Override
    public abstract boolean isFieldBoostSupported();

    @Override
    public abstract SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

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
}
