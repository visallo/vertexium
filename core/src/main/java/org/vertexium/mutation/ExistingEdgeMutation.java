package org.vertexium.mutation;

import org.vertexium.Edge;
import org.vertexium.util.IncreasingTime;

public abstract class ExistingEdgeMutation extends ExistingElementMutationImpl<Edge> implements EdgeMutation {
    private String newEdgeLabel;
    private long alterEdgeLabelTimestamp;

    public ExistingEdgeMutation(Edge edge) {
        super(edge);
    }

    @Override
    public EdgeMutation alterEdgeLabel(String newEdgeLabel) {
        this.newEdgeLabel = newEdgeLabel;
        alterEdgeLabelTimestamp = IncreasingTime.currentTimeMillis();
        return this;
    }

    @Override
    public long getAlterEdgeLabelTimestamp() {
        return alterEdgeLabelTimestamp;
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
