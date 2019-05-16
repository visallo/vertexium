package org.vertexium.mutation;

import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.EdgeElementLocation;

public interface EdgeMutation extends ElementMutation<Edge>, EdgeElementLocation {
    EdgeMutation alterEdgeLabel(String newEdgeLabel);

    String getNewEdgeLabel();

    long getAlterEdgeLabelTimestamp();

    String getVertexId(Direction direction);

    String getEdgeLabel();
}
