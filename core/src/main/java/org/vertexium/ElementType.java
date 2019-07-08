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

    public static ElementType parse(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ElementType) {
            return (ElementType) value;
        }
        if (value instanceof String) {
            return ElementType.valueOf(((String) value).toUpperCase());
        }
        throw new VertexiumException("Could not parse element type: " + value);
    }
}
