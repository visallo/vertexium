package org.vertexium;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    private Visibility visibility;

    public VertexBuilder(String vertexId, Visibility visibility) {
        super(vertexId);
        this.visibility = visibility;
    }

    /**
     * Save the vertex along with any properties that were set to the graph.
     *
     * @return The newly created vertex.
     */
    @Override
    public abstract Vertex save(Authorizations authorizations);

    public String getVertexId() {
        return getElementId();
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
