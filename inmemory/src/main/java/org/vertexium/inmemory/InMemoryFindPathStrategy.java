package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.util.ArrayUtils;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

class InMemoryFindPathStrategy {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(InMemoryFindPathStrategy.class);
    private final InMemoryGraph graph;

    public InMemoryFindPathStrategy(InMemoryGraph graph) {
        this.graph = graph;
    }

    public Stream<Path> findPaths(FindPathOptions options, User user) {
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
        Vertex sourceVertex = graph.getVertex(options.getSourceVertexId(), fetchHints, user);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getSourceVertexId());
        }
        Vertex destVertex = graph.getVertex(options.getDestVertexId(), fetchHints, user);
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
                user
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
                user
            );
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths.stream();
    }

    private void findPathsSetIntersection(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        ProgressCallback progressCallback,
        User user
    ) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = filterFindPathEdgeInfo(options, sourceVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> sourceVerticesExist = graph.doVerticesExist(sourceVertexConnectedVertexIds, user);
        sourceVertexConnectedVertexIds = stream(sourceVerticesExist.keySet())
            .filter(key -> sourceVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = filterFindPathEdgeInfo(options, destVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> destVerticesExist = graph.doVerticesExist(destVertexConnectedVertexIds, user);
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

    private void findPathsRecursive(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        int hops,
        Set<String> seenVertices,
        Path currentPath,
        ProgressCallback progressCallback,
        User user
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
            Iterable<Vertex> vertices = filterFindPathEdgePairs(options, sourceVertex.getEdgeVertexPairs(Direction.BOTH, options.getLabels(), user));
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
                    findPathsRecursive(options, foundPaths, child, destVertex, hops - 1, seenVertices, new Path(currentPath, child.getId()), progressCallback, user);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }

    private Set<String> filterFindPathEdgeInfo(FindPathOptions options, Stream<EdgeInfo> edgeInfos) {
        return edgeInfos
            .filter(edgeInfo -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgeInfo.getLabel());
                }
                return true;
            })
            .map(EdgeInfo::getVertexId)
            .collect(Collectors.toSet());
    }

    private Iterable<Vertex> filterFindPathEdgePairs(FindPathOptions options, Stream<EdgeVertexPair> edgeVertexPairs) {
        return edgeVertexPairs
            .filter(edgePair -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgePair.getEdge().getLabel());
                }
                return true;
            })
            .map(EdgeVertexPair::getVertex)
            .collect(Collectors.toList());
    }
}
