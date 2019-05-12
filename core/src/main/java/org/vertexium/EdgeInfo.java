package org.vertexium;

public interface EdgeInfo {
    String getEdgeId();

    String getLabel();

    String getVertexId();

    Direction getDirection();

    Visibility getVisibility();
}
