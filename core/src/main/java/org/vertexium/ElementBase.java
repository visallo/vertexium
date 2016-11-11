package org.vertexium;

import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.property.PropertyValue;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.PropertyCollection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class ElementBase implements Element {
    private final Graph graph;
    private final String id;
    private Property idProperty;
    private Property edgeLabelProperty;
    private Visibility visibility;
    private final long timestamp;
    private Set<Visibility> hiddenVisibilities = new HashSet<>();

    private final PropertyCollection properties;
    private ConcurrentSkipListSet<PropertyDeleteMutation> propertyDeleteMutations;
    private ConcurrentSkipListSet<PropertySoftDeleteMutation> propertySoftDeleteMutations;
    private final Authorizations authorizations;

    protected ElementBase(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long timestamp,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
        this.timestamp = timestamp;
        this.properties = new PropertyCollection();
        this.authorizations = authorizations;
        if (hiddenVisibilities != null) {
            for (Visibility v : hiddenVisibilities) {
                this.hiddenVisibilities.add(v);
            }
        }
        updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
    }

    @Override
    public Iterable<Object> getPropertyValues(final String name) {
        return new ConvertingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Iterable<Object> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
        for (Property p : getProperties(name)) {
            if (!p.getKey().equals(key)) {
                continue;
            }
            if (visibility == null) {
                return p;
            }
            if (!visibility.equals(p.getVisibility())) {
                continue;
            }
            return p;
        }
        return null;
    }

    @Override
    public Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

    @Override
    public Property getProperty(String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    @Override
    public Object getPropertyValue(String name) {
        return getPropertyValue(name, 0);
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
        Property property = this.properties.getProperty(name, index);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
        Property property = this.properties.getProperty(key, name, index);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        return getPropertyValue(key, name, 0);
    }

    @Override
    public String getId() {
        return this.id;
    }

    protected Property getIdProperty() {
        if (idProperty == null) {
            idProperty = new MutablePropertyImpl(ElementMutation.DEFAULT_KEY, ID_PROPERTY_NAME, getId(), null, getTimestamp(), null, null);
        }
        return idProperty;
    }

    protected Property getEdgeLabelProperty() {
        if (edgeLabelProperty == null && this instanceof Edge) {
            String edgeLabel = ((Edge) this).getLabel();
            edgeLabelProperty = new MutablePropertyImpl(ElementMutation.DEFAULT_KEY, Edge.LABEL_PROPERTY_NAME, edgeLabel, null, getTimestamp(), null, null);
        }
        return edgeLabelProperty;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    protected void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public Iterable<Property> getProperties() {
        return this.properties.getProperties();
    }

    public Iterable<PropertyDeleteMutation> getPropertyDeleteMutations() {
        return this.propertyDeleteMutations;
    }

    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeleteMutations() {
        return this.propertySoftDeleteMutations;
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getIdProperty());
            return result;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getEdgeLabelProperty());
            return result;
        }
        return this.properties.getProperties(name);
    }

    @Override
    public Iterable<Property> getProperties(final String key, final String name) {
        if (ID_PROPERTY_NAME.equals(name) || (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge)) {
            return getProperties(name);
        }
        return this.properties.getProperties(key, name);
    }

    // this method differs setProperties in that it only updates the in memory representation of the properties
    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations
    ) {
        if (propertyDeleteMutations != null) {
            this.propertyDeleteMutations = new ConcurrentSkipListSet<>();
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
                removePropertyInternal(
                        propertyDeleteMutation.getKey(),
                        propertyDeleteMutation.getName(),
                        propertyDeleteMutation.getVisibility()
                );
                this.propertyDeleteMutations.add(propertyDeleteMutation);
            }
        }
        if (propertySoftDeleteMutations != null) {
            this.propertySoftDeleteMutations = new ConcurrentSkipListSet<>();
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
                removePropertyInternal(
                        propertySoftDeleteMutation.getKey(),
                        propertySoftDeleteMutation.getName(),
                        propertySoftDeleteMutation.getVisibility()
                );
                this.propertySoftDeleteMutations.add(propertySoftDeleteMutation);
            }
        }

        for (Property property : properties) {
            addPropertyInternal(property);
        }
    }

    protected void addPropertyInternal(Property property) {
        if (property.getKey() == null) {
            throw new IllegalArgumentException("key is required for property");
        }
        Object propertyValue = property.getValue();
        if (propertyValue instanceof PropertyValue && !((PropertyValue) propertyValue).isStore()) {
            return;
        }
        Property existingProperty = getProperty(property.getKey(), property.getName(), property.getVisibility());
        if (existingProperty == null) {
            this.properties.addProperty(property);
        } else {
            if (existingProperty instanceof MutableProperty) {
                ((MutableProperty) existingProperty).update(property);
            } else {
                throw new VertexiumException("Could not update property of type: " + existingProperty.getClass().getName());
            }
        }
    }

    protected Property removePropertyInternal(String key, String name, Visibility visibility) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
        }
        return property;
    }

    protected Property removePropertyInternal(String key, String name) {
        Property property = getProperty(key, name);
        if (property != null) {
            this.properties.removeProperty(property);
        }
        return property;
    }

    protected Property softDeletePropertyInternal(String key, String name) {
        Property property = getProperty(key, name);
        if (property != null) {
            this.properties.removeProperty(property);
        }
        return property;
    }

    protected Property softDeletePropertyInternal(String key, String name, Visibility visibility) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
        }
        return property;
    }

    protected Iterable<Property> removePropertyInternal(String name) {
        return this.properties.removeProperties(name);
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Element) {
            Element objElem = (Element) obj;
            return getId().equals(objElem.getId());
        }
        return super.equals(obj);
    }

    @Override
    public abstract void deleteProperty(String key, String name, Authorizations authorizations);

    @Override
    public abstract void deleteProperties(String name, Authorizations authorizations);

    @Override
    public abstract void softDeleteProperty(String key, String name, Authorizations authorizations);

    @Override
    public abstract void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void softDeleteProperties(String name, Authorizations authorizations);

    @Override
    public void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, visibility).save(authorizations);
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, metadata, visibility).save(authorizations);
    }

    @Override
    public void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, visibility).save(authorizations);
    }

    @Override
    public void setProperty(String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, metadata, visibility).save(authorizations);
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyHidden(property, timestamp, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(key, name, propertyVisibility, null, visibility, authorizations);
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property, null, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyVisible(property, timestamp, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(key, name, propertyVisibility, null, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(property, null, visibility, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public abstract void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations);

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void mergeProperties(Element element) {
        for (Property property : element.getProperties()) {
            this.properties.removeProperty(property);
            this.properties.addProperty(property);
        }
    }

    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        for (Visibility visibility : getHiddenVisibilities()) {
            if (authorizations.canRead(visibility)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public abstract void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations);

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Long startTime, Long endTime, Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, null, startTime, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        return getHistoricalPropertyValues(key, name, visibility, null, null, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, final Long startTime, final Long endTime, Authorizations authorizations) {
        return new FilterIterable<HistoricalPropertyValue>(getHistoricalPropertyValues(key, name, visibility, authorizations)) {
            @Override
            protected boolean isIncluded(HistoricalPropertyValue pv) {
                if (startTime != null && pv.getTimestamp() < startTime) {
                    return false;
                }
                if (endTime != null && pv.getTimestamp() > endTime) {
                    return false;
                }
                return true;
            }
        };
    }
}
