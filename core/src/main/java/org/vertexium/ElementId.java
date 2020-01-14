package org.vertexium;

public interface ElementId extends VertexiumObjectId {
    static ElementId vertex(String id) {
        return new DefaultElementId(ElementType.VERTEX, id);
    }

    static ElementId edge(String id) {
        return new DefaultElementId(ElementType.EDGE, id);
    }

    static ElementId create(ElementType elementType, String id) {
        return new DefaultElementId(elementType, id);
    }

    /**
     * the type of the element.
     */
    ElementType getElementType();

    /**
     * id of the element.
     */
    String getId();
}
