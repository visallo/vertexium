package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.Property;
import org.vertexium.Visibility;

public class MarkHiddenPropertyEvent extends GraphEvent {
    private final Element element;
    private final Property property;
    private final Visibility visibility;

    public MarkHiddenPropertyEvent(Graph graph, Element element, Property property, Visibility visibility) {
        super(graph);
        this.element = element;
        this.property = property;
        this.visibility = visibility;
    }

    public Element getElement() {
        return element;
    }

    public Property getProperty() {
        return property;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "MarkHiddenPropertyEvent{element=" + element + '}';
    }

    @Override
    public int hashCode() {
        return getElement().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MarkHiddenPropertyEvent)) {
            return false;
        }

        MarkHiddenPropertyEvent other = (MarkHiddenPropertyEvent) obj;
        return getElement().equals(other.getElement());
    }
}
