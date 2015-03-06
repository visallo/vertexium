package org.neolumin.vertexium;

public abstract class EdgeBuilderByVertexId extends EdgeBuilderBase {
    private final String outVertexId;
    private final String inVertexId;

    protected EdgeBuilderByVertexId(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        super(edgeId, label, visibility);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }
}
