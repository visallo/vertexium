package org.vertexium.mutation;

import org.vertexium.Edge;

public interface EdgeMutation extends ElementMutation<Edge> {
    EdgeMutation alterEdgeLabel(String newEdgeLabel);

    String getNewEdgeLabel();
}
