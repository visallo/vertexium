package org.vertexium;

abstract class DefaultElementLocation extends DefaultElementId implements ElementLocation {
    private final Visibility visibility;

    protected DefaultElementLocation(ElementType elementType, String id, Visibility visibility) {
        super(elementType, id);
        this.visibility = visibility;
    }

    @Override
    public Visibility getVisibility() {
        return visibility;
    }
}
