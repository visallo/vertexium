package org.vertexium;

class DefaultVertexElementLocation extends DefaultElementLocation implements VertexElementLocation {
    protected DefaultVertexElementLocation(String id, Visibility visibility) {
        super(ElementType.VERTEX, id, visibility);
    }
}
