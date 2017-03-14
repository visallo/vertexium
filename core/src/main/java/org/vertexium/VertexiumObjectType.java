package org.vertexium;

import java.util.EnumSet;

public enum VertexiumObjectType {
    VERTEX,
    EDGE,
    EXTENDED_DATA;

    public static final EnumSet<VertexiumObjectType> ALL = EnumSet.allOf(VertexiumObjectType.class);
    public static final EnumSet<VertexiumObjectType> ELEMENTS = EnumSet.of(VERTEX, EDGE);

    public static VertexiumObjectType getTypeFromElement(Element element) {
        if (element instanceof Vertex) {
            return VERTEX;
        }
        if (element instanceof Edge) {
            return EDGE;
        }
        throw new VertexiumException("Unhandled element type: " + element);
    }
}
