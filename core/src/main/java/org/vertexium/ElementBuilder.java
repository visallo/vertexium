package org.vertexium;

import org.vertexium.mutation.ElementMutationBase;
import org.vertexium.util.KeyUtils;

public abstract class ElementBuilder<T extends Element> extends ElementMutationBase<T, ElementBuilder<T>> {
    private final ElementType elementType;
    private final String elementId;
    private final Visibility elementVisibility;

    protected ElementBuilder(ElementType elementType, String elementId, Visibility elementVisibility) {
        KeyUtils.checkKey(elementId, "Invalid elementId");
        this.elementType = elementType;
        this.elementId = elementId;
        this.elementVisibility = elementVisibility;
    }

    @Override
    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public Visibility getVisibility() {
        return elementVisibility;
    }

    @Override
    public String getId() {
        return elementId;
    }

    /**
     * @deprecated use {@link #getId()}
     */
    @Deprecated
    public String getElementId() {
        return elementId;
    }

    @Override
    protected FetchHints getFetchHints() {
        return FetchHints.ALL;
    }
}
