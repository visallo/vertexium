package org.neolumin.vertexium.event;

import org.neolumin.vertexium.Element;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Property;

public class AddPropertyEvent extends GraphEvent {
    private final Element element;
    private final Property property;

    public AddPropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.property = property;
    }

    public Element getElement() {
        return element;
    }

    public Property getProperty() {
        return property;
    }

    @Override
    public int hashCode() {
        return getProperty().hashCode();
    }

    @Override
    public String toString() {
        return "AddPropertyEvent{element=" + getElement() + ", property=" + property + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AddPropertyEvent)) {
            return false;
        }

        AddPropertyEvent other = (AddPropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getProperty().equals(other.getProperty())
                && super.equals(obj);
    }
}
