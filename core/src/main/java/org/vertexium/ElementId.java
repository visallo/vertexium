package org.vertexium;

public class ElementId {
    private final ElementType elementType;
    private final String elementId;

    public ElementId(ElementType elementType, String elementId) {
        this.elementType = elementType;
        this.elementId = elementId;
    }

    public static ElementId vertex(String id) {
        return new ElementId(ElementType.VERTEX, id);
    }

    public static ElementId edge(String id) {
        return new ElementId(ElementType.EDGE, id);
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getElementId() {
        return elementId;
    }

    @Override
    public String toString() {
        return String.format("ElementId{elementType=%s, elementId='%s'}", elementType, elementId);
    }
}
