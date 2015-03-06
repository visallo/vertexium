package org.neolumin.vertexium.path;

import org.neolumin.vertexium.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.neolumin.vertexium.util.IterableUtils.toList;
import static org.neolumin.vertexium.util.IterableUtils.toSet;

// TODO this algorithm could probably be better optimized to be iterable instead
public class RecursivePathFindingAlgorithm implements PathFindingAlgorithm {
    @Override
    public Iterable<Path> findPaths(Graph graph, final Vertex sourceVertex, Vertex destVertex, int hops, ProgressCallback progressCallback, final Authorizations authorizations) {
        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        Set<String> seenVertices = new HashSet<>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<>();
        if (hops == 2) {
            findPathsSetIntersection(foundPaths, sourceVertex, destVertex, progressCallback, authorizations);
        } else {
            findPathsRecursive(foundPaths, sourceVertex, destVertex, hops, hops, seenVertices, startPath, progressCallback, authorizations);
        }
        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths;
    }

    private void findPathsSetIntersection(List<Path> foundPaths, Vertex sourceVertex, Vertex destVertex, ProgressCallback progressCallback, Authorizations authorizations) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = toSet(sourceVertex.getVertexIds(Direction.BOTH, authorizations));

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = toSet(destVertex.getVertexIds(Direction.BOTH, authorizations));

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private void findPathsRecursive(List<Path> foundPaths, final Vertex sourceVertex, Vertex destVertex, int hops, int totalHops, Set<String> seenVertices, Path currentPath, ProgressCallback progressCallback, final Authorizations authorizations) {
        // if this is our first source vertex report progress back to the progress callback
        boolean firstLevelRecursion = hops == totalHops;

        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Iterable<Vertex> vertices = sourceVertex.getVertices(Direction.BOTH, authorizations);
            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertices = toList(vertices);
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
                    findPathsRecursive(foundPaths, child, destVertex, hops - 1, totalHops, seenVertices, new Path(currentPath, child.getId()), progressCallback, authorizations);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }
}
