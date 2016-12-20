package org.vertexium;

import org.vertexium.mutation.EdgeMutation;

public abstract class EdgeBuilderBase extends ElementBuilder<Edge> implements EdgeMutation {
    private final String edgeId;
    private final String label;
    private final Visibility visibility;
    private String newEdgeLabel;

    protected EdgeBuilderBase(String edgeId, String label, Visibility visibility) {
        this.edgeId = edgeId;
        this.label = label;
        this.visibility = visibility;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public String getLabel() {
        return label;
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
