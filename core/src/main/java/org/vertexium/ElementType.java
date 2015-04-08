package org.vertexium;

public enum ElementType {
    VERTEX,
    EDGE;

    public static ElementType getTypeFromElement(Element element) {
        if (element instanceof Vertex) {
            return VERTEX;
        }
        if (element instanceof Edge) {
            return EDGE;
        }
        throw new VertexiumException("Unhandled element type: " + element);
    }
}
