package org.vertexium;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    public VertexBuilder(String vertexId, Visibility visibility) {
        super(ElementType.VERTEX, vertexId, visibility);
    }
}
