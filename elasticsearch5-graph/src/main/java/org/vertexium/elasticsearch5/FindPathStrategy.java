package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.query.Contains;
import org.vertexium.query.Query;
import org.vertexium.query.QueryResultsIterable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FindPathStrategy {
    private final Elasticsearch5Graph graph;
    private final FindPathOptions options;
    private final ProgressCallback progressCallback;
    private final User user;

    public FindPathStrategy(
        Elasticsearch5Graph graph,
        FindPathOptions options,
        ProgressCallback progressCallback,
        User user
    ) {
        this.graph = graph;
        this.options = options;
        this.progressCallback = progressCallback;
        this.user = user;
    }

    public Stream<Path> findPaths() {
        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() < 1) {
            throw new IllegalArgumentException("maxHops cannot be less than 1");
        } else if (options.getMaxHops() == 1) {
            Set<String> sourceConnectedVertexIds = getConnectedVertexIds(options.getSourceVertexId());
            if (sourceConnectedVertexIds.contains(options.getDestVertexId())) {
                foundPaths.add(new Path(options.getSourceVertexId(), options.getDestVertexId()));
            }
        } else if (options.getMaxHops() == 2) {
            findPathsSetIntersection(foundPaths);
        } else {
            findPathsBreadthFirst(foundPaths, options.getSourceVertexId(), options.getDestVertexId(), options.getMaxHops());
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths.stream();
    }

    private void findPathsSetIntersection(List<Path> foundPaths) {
        String sourceVertexId = options.getSourceVertexId();
        String destVertexId = options.getDestVertexId();

        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(sourceVertexId);
        vertexIds.add(destVertexId);
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(vertexIds);

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = connectedVertexIds.get(sourceVertexId);
        if (sourceVertexConnectedVertexIds == null) {
            return;
        }

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = connectedVertexIds.get(destVertexId);
        if (destVertexConnectedVertexIds == null) {
            return;
        }

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        foundPaths.addAll(
            sourceVertexConnectedVertexIds.stream()
                .map(connectedVertexId -> new Path(sourceVertexId, connectedVertexId, destVertexId))
                .collect(Collectors.toList())
        );
    }

    private void findPathsBreadthFirst(List<Path> foundPaths, String sourceVertexId, String destVertexId, int hops) {
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(sourceVertexId, destVertexId);
        // start at 2 since we already got the source and dest vertex connected vertex ids
        for (int i = 2; i < hops; i++) {
            progressCallback.progress((double) i / (double) hops, ProgressCallback.Step.FINDING_PATH);
            Set<String> vertexIdsToSearch = new HashSet<>();
            for (Map.Entry<String, Set<String>> entry : connectedVertexIds.entrySet()) {
                vertexIdsToSearch.addAll(entry.getValue());
            }
            vertexIdsToSearch.removeAll(connectedVertexIds.keySet());
            Map<String, Set<String>> r = getConnectedVertexIds(vertexIdsToSearch);
            connectedVertexIds.putAll(r);
        }
        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        Set<String> seenVertices = new HashSet<>();
        Path currentPath = new Path(sourceVertexId);
        findPathsRecursive(connectedVertexIds, foundPaths, sourceVertexId, destVertexId, hops, seenVertices, currentPath, progressCallback);
    }

    private void findPathsRecursive(
        Map<String, Set<String>> connectedVertexIds,
        List<Path> foundPaths,
        final String sourceVertexId,
        String destVertexId,
        int hops,
        Set<String> seenVertices,
        Path currentPath,
        @SuppressWarnings("UnusedParameters") ProgressCallback progressCallback
    ) {
        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }
        seenVertices.add(sourceVertexId);
        if (sourceVertexId.equals(destVertexId)) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Set<String> vertexIds = connectedVertexIds.get(sourceVertexId);
            if (vertexIds != null) {
                for (String childId : vertexIds) {
                    if (!seenVertices.contains(childId)) {
                        findPathsRecursive(connectedVertexIds, foundPaths, childId, destVertexId, hops - 1, seenVertices, new Path(currentPath, childId), progressCallback);
                    }
                }
            }
        }
        seenVertices.remove(sourceVertexId);
    }

    private Set<String> getConnectedVertexIds(String vertexId) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId);
        Map<String, Set<String>> results = getConnectedVertexIds(vertexIds);
        Set<String> vertexIdResults = results.get(vertexId);
        if (vertexIdResults == null) {
            return new HashSet<>();
        }
        return vertexIdResults;
    }

    private Map<String, Set<String>> getConnectedVertexIds(String vertexId1, String vertexId2) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId1);
        vertexIds.add(vertexId2);
        return getConnectedVertexIds(vertexIds);
    }

    private Map<String, Set<String>> getConnectedVertexIds(Set<String> vertexIds) {
        Query query = graph.query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .has(Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME, Contains.IN, vertexIds);
        if (options.getLabels() != null) {
            query = query.has(Edge.LABEL_PROPERTY_NAME, Contains.IN, options.getLabels());
        }
        if (options.getExcludedLabels() != null) {
            query = query.has(Edge.LABEL_PROPERTY_NAME, Contains.NOT_IN, options.getExcludedLabels());
        }
        QueryResultsIterable<Edge> edges = query
            .edges(FetchHints.NONE);
        Map<String, Set<String>> results = new HashMap<>();
        for (Edge edge : edges) {
            String outVertexId = edge.getVertexId(Direction.OUT);
            String inVertexId = edge.getVertexId(Direction.IN);
            if (vertexIds.contains(outVertexId)) {
                Set<String> list = results.computeIfAbsent(outVertexId, s -> new HashSet<>());
                list.add(inVertexId);
            }
            if (vertexIds.contains(inVertexId)) {
                Set<String> list = results.computeIfAbsent(inVertexId, s -> new HashSet<>());
                list.add(outVertexId);
            }
        }
        return results;
    }
}
