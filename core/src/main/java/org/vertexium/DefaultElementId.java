package org.vertexium;

public class DefaultElementId implements ElementId {
    private final ElementType elementType;
    private final String id;

    public DefaultElementId(ElementType elementType, String id) {
        this.elementType = elementType;
        this.id = id;
    }

    @Override
    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ElementId) {
            ElementId objElementId = (ElementId) obj;
            return getId().equals(objElementId.getId()) && getElementType().equals(objElementId.getElementType());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }
}
