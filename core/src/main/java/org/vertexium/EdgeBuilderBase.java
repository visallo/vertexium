package org.vertexium;

import org.vertexium.mutation.EdgeMutation;
import org.vertexium.util.IncreasingTime;

public abstract class EdgeBuilderBase extends ElementBuilder<Edge> implements EdgeMutation {
    private final String label;
    private final Visibility visibility;
    private String newEdgeLabel;
    private long alterEdgeLabelTimestamp;

    protected EdgeBuilderBase(String edgeId, String label, Visibility visibility) {
        super(edgeId);
        this.label = label;
        this.visibility = visibility;
        this.alterEdgeLabelTimestamp = IncreasingTime.currentTimeMillis();
    }

    public String getEdgeId() {
        return getElementId();
    }

    public String getLabel() {
        return label;
    }

    @Override
    public long getAlterEdgeLabelTimestamp() {
        return alterEdgeLabelTimestamp;
    }

    public Visibility getVisibility() {
        return visibility;
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

    /**
     * Save the edge along with any properties that were set to the graph.
     *
     * @return The newly created edge.
     */
    @Override
    public abstract Edge save(Authorizations authorizations);

    public abstract String getOutVertexId();

    public abstract String getInVertexId();

    @Override
    public boolean hasChanges() {
        if (newEdgeLabel != null) {
            return true;
        }

        return super.hasChanges();
    }
}
