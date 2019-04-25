package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.Property;
import org.vertexium.Visibility;

public class MarkVisiblePropertyEvent extends GraphEvent {
    private final Element element;
    private final Property property;
    private final Visibility visibility;
    private final Object data;

    public MarkVisiblePropertyEvent(Graph graph, Element element, Property property, Visibility visibility, Object data) {
        super(graph);
        this.element = element;
        this.property = property;
        this.visibility = visibility;
        this.data = data;
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

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "MarkVisiblePropertyEvent{element=" + element + '}';
    }

    @Override
    public int hashCode() {
        return getElement().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MarkVisiblePropertyEvent)) {
            return false;
        }

        MarkVisiblePropertyEvent other = (MarkVisiblePropertyEvent) obj;
        return getElement().equals(other.getElement());
    }
}
