package org.vertexium;

public abstract class EdgeBuilderByVertexId extends EdgeBuilderBase {
    protected EdgeBuilderByVertexId(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility
    ) {
        super(edgeId, outVertexId, inVertexId, label, visibility);
    }
}
