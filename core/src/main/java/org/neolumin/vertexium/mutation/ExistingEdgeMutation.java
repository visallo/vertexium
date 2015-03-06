package org.neolumin.vertexium.mutation;

import org.neolumin.vertexium.Edge;

public abstract class ExistingEdgeMutation extends ExistingElementMutationImpl<Edge> implements EdgeMutation {
    private String newEdgeLabel;

    public ExistingEdgeMutation(Edge edge) {
        super(edge);
    }

    @Override
    public EdgeMutation alterEdgeLabel(String newEdgeLabel) {
        this.newEdgeLabel = newEdgeLabel;
        return this;
    }

    @Override
    public String getNewEdgeLabel() {
        return newEdgeLabel;
    }
}
