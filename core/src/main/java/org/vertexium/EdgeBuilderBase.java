package org.vertexium;

import org.vertexium.mutation.EdgeMutation;
import org.vertexium.util.IncreasingTime;

public abstract class EdgeBuilderBase extends ElementBuilder<Edge> implements EdgeMutation {
    private final String outVertexId;
    private final String inVertexId;
    private final String label;
    private String newEdgeLabel;
    private long alterEdgeLabelTimestamp;

    protected EdgeBuilderBase(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility
    ) {
        super(ElementType.EDGE, edgeId, visibility);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.alterEdgeLabelTimestamp = IncreasingTime.currentTimeMillis();
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new VertexiumException("unhandled direction: " + direction);
        }
    }

    /**
     * @deprecated Use {@link #getId()}
     */
    @Deprecated
    public String getEdgeId() {
        return getElementId();
    }

    public String getEdgeLabel() {
        return label;
    }

    @Override
    public long getAlterEdgeLabelTimestamp() {
        return alterEdgeLabelTimestamp;
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

    @Override
    public boolean hasChanges() {
        if (newEdgeLabel != null) {
            return true;
        }

        return super.hasChanges();
    }
}
