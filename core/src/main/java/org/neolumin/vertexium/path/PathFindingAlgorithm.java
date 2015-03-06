package org.neolumin.vertexium.path;

import org.neolumin.vertexium.*;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(Graph graph, Vertex sourceVertex, Vertex destVertex, int hops, ProgressCallback progressCallback, Authorizations authorizations);
}
