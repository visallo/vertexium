package org.vertexium.accumulo;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.ConnectedVertexIdsIterator;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class AccumuloFindPathStrategy {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloFindPathStrategy.class);
    private final AccumuloGraph graph;
    private final FindPathOptions options;
    private final ProgressCallback progressCallback;
    private final Authorizations authorizations;

    public AccumuloFindPathStrategy(
            AccumuloGraph graph,
            FindPathOptions options,
            ProgressCallback progressCallback,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.options = options;
        this.progressCallback = progressCallback;
        this.authorizations = authorizations;
    }

    public Iterable<Path> findPaths() {
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
        return foundPaths;
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
        Span trace = Trace.start("getConnectedVertexIds");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("getConnectedVertexIds:\n  %s", IterableUtils.join(vertexIds, "\n  "));
            }

            if (vertexIds.size() == 0) {
                return new HashMap<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIds) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            int maxVersions = 1;
            Long startTime = null;
            Long endTime = null;
            ScannerBase scanner = graph.createElementScanner(
                    FetchHint.EDGE_REFS,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting connectedVertexIdsIteratorSettings = new IteratorSetting(
                    1000,
                    ConnectedVertexIdsIterator.class.getSimpleName(),
                    ConnectedVertexIdsIterator.class
            );
            ConnectedVertexIdsIterator.setLabels(connectedVertexIdsIteratorSettings, options.getLabels());
            ConnectedVertexIdsIterator.setExcludedLabels(connectedVertexIdsIteratorSettings, options.getExcludedLabels());
            scanner.addScanIterator(connectedVertexIdsIteratorSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Map<String, Set<String>> results = new HashMap<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    try {
                        Set<String> rowVertexIds = ConnectedVertexIdsIterator.decodeValue(row.getValue());
                        results.put(row.getKey().getRow().toString(), rowVertexIds);
                    } catch (IOException e) {
                        throw new VertexiumException("Could not decode vertex ids for row: " + row.getKey().toString(), e);
                    }
                }
                return results;
            } finally {
                scanner.close();
                AccumuloGraph.GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }
}
