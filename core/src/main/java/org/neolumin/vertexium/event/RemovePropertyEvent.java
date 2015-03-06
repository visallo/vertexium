package org.neolumin.vertexium.event;

import org.neolumin.vertexium.Element;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;

public class RemovePropertyEvent extends GraphEvent {
    private final Element element;
    private final String key;
    private final String name;
    private final Visibility visibility;

    public RemovePropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.key = property.getKey();
        this.name = property.getName();
        this.visibility = property.getVisibility();
    }

    public RemovePropertyEvent(Graph graph, Element element, PropertyRemoveMutation propertyRemoveMutation) {
        super(graph);
        this.element = element;
        this.key = propertyRemoveMutation.getKey();
        this.name = propertyRemoveMutation.getName();
        this.visibility = propertyRemoveMutation.getVisibility();
    }

    public Element getElement() {
        return element;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode() ^ getName().hashCode() ^ getVisibility().hashCode();
    }

    @Override
    public String toString() {
        return "RemovePropertyEvent{element=" + getElement() + ", property=" + getKey() + ":" + getName() + ":" + getVisibility() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemovePropertyEvent)) {
            return false;
        }

        RemovePropertyEvent other = (RemovePropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getKey().equals(other.getKey())
                && getName().equals(other.getName())
                && getVisibility().equals(other.getVisibility())
                && super.equals(obj);
    }
}
