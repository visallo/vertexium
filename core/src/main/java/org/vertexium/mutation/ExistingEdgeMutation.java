package org.vertexium.mutation;

import org.vertexium.Edge;

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

    @Override
    public boolean hasChanges() {
        if (newEdgeLabel != null) {
            return true;
        }

        return super.hasChanges();
    }
}
