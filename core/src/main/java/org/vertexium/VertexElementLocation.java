package org.vertexium;

public interface VertexElementLocation extends ElementLocation {
    static VertexElementLocation create(String vertexId, Visibility visibility) {
        return new VertexElementLocation() {
            @Override
            public ElementType getElementType() {
                return ElementType.VERTEX;
            }

            @Override
            public String getId() {
                return vertexId;
            }

            @Override
            public Visibility getVisibility() {
                return visibility;
            }
        };
    }
}
