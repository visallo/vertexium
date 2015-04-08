package org.vertexium.path;

import org.vertexium.*;
import org.vertexium.*;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(Graph graph, Vertex sourceVertex, Vertex destVertex, int hops, ProgressCallback progressCallback, Authorizations authorizations);
}
