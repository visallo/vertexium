package org.neolumin.vertexium.mutation;

import org.neolumin.vertexium.Edge;

public interface EdgeMutation extends ElementMutation<Edge> {
    EdgeMutation alterEdgeLabel(String newEdgeLabel);

    String getNewEdgeLabel();
}
