package org.vertexium.mutation;

import org.vertexium.*;

public abstract class ExistingElementMutationImpl<T extends Element>
    extends ElementMutationBase<T, ExistingElementMutation<T>>
    implements ElementMutation<T>, ExistingElementMutation<T> {
    private final T element;
    private Visibility oldElementVisibility;

    public ExistingElementMutationImpl(T element) {
        this.element = element;
        if (element != null) {
            this.oldElementVisibility = element.getVisibility();
        }
    }

    @Override
    public ElementType getElementType() {
        return ElementType.getTypeFromElement(getElement());
    }

    @Override
    public String getId() {
        return getElement().getId();
    }

    @Override
    public Visibility getVisibility() {
        return getElement().getVisibility();
    }

    @Override
    public T getElement() {
        return element;
    }

    @Override
    public Visibility getOldElementVisibility() {
        return oldElementVisibility;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String name) {
        for (Property prop : getElement().getProperties(name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String key, String name) {
        for (Property prop : getElement().getProperties(key, name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    protected FetchHints getFetchHints() {
        return getElement().getFetchHints();
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String name, Object eventData) {
        for (Property prop : getElement().getProperties(name)) {
            softDeleteProperty(prop, eventData);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String key, String name, Object eventData) {
        for (Property prop : getElement().getProperties(key, name)) {
            softDeleteProperty(prop, eventData);
        }
        return this;
    }
}
