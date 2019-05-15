package org.vertexium.mutation;

import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.util.IncreasingTime;

public abstract class ExistingEdgeMutation extends ExistingElementMutationBase<Edge> implements EdgeMutation {
    private String newEdgeLabel;
    private long alterEdgeLabelTimestamp;

    public ExistingEdgeMutation(Edge edge) {
        super(edge);
    }

    @Override
    public String getVertexId(Direction direction) {
        return getElement().getVertexId(direction);
    }

    @Override
    public String getEdgeLabel() {
        if (getNewEdgeLabel() != null) {
            return getNewEdgeLabel();
        }
        return getElement().getLabel();
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
