package org.vertexium;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    public VertexBuilder(String vertexId, Visibility visibility) {
        super(ElementType.VERTEX, vertexId, visibility);
    }

    /**
     * Save the vertex along with any properties that were set to the graph.
     *
     * @return The newly created vertex.
     */
    @Override
    public abstract Vertex save(Authorizations authorizations);
}
