package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.mutation.PropertySoftDeleteMutation;

public class SoftDeletePropertyEvent extends GraphEvent {
    private final Element element;
    private final String key;
    private final String name;
    private final Visibility visibility;

    public SoftDeletePropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.key = property.getKey();
        this.name = property.getName();
        this.visibility = property.getVisibility();
    }

    public SoftDeletePropertyEvent(Graph graph, Element element, PropertySoftDeleteMutation propertySoftDeleteMutation) {
        super(graph);
        this.element = element;
        this.key = propertySoftDeleteMutation.getKey();
        this.name = propertySoftDeleteMutation.getName();
        this.visibility = propertySoftDeleteMutation.getVisibility();
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
        return this.getClass().getSimpleName() + "{element=" + getElement() + ", property=" + getKey() + ":" + getName() + ":" + getVisibility() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SoftDeletePropertyEvent)) {
            return false;
        }

        SoftDeletePropertyEvent other = (SoftDeletePropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getKey().equals(other.getKey())
                && getName().equals(other.getName())
                && getVisibility().equals(other.getVisibility());
    }
}
