package org.vertexium;

public interface EdgeElementLocation extends ElementLocation {
    String getVertexId(Direction direction);

    String getLabel();
}
