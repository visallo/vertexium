package org.neolumin.vertexium;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    private String vertexId;
    private Visibility visibility;

    public VertexBuilder(String vertexId, Visibility visibility) {
        this.vertexId = vertexId;
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
        return vertexId;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
