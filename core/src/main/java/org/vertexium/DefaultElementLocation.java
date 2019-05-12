package org.vertexium;

abstract class DefaultElementLocation implements ElementLocation {
    private final ElementType elementType;
    private final String id;
    private final Visibility visibility;

    protected DefaultElementLocation(ElementType elementType, String id, Visibility visibility) {
        this.elementType = elementType;
        this.id = id;
        this.visibility = visibility;
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
    public Visibility getVisibility() {
        return visibility;
    }
}
