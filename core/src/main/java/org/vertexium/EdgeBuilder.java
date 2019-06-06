package org.vertexium;

public abstract class EdgeBuilder extends EdgeBuilderBase {
    private Vertex outVertex;
    private Vertex inVertex;

    public EdgeBuilder(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        super(edgeId, outVertex.getId(), inVertex.getId(), label, visibility);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    protected Vertex getOutVertex() {
        return outVertex;
    }

    protected Vertex getInVertex() {
        return inVertex;
    }

    @Override
    public abstract Edge save(Authorizations authorizations);
}
