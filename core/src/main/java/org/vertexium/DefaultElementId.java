package org.vertexium;

import java.util.Objects;

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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultElementId that = (DefaultElementId) o;
        return elementType == that.elementType
            && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementType, id);
    }

    @Override
    public String toString() {
        return String.format("ElementId{elementType=%s, id='%s'}", elementType, id);
    }
}
