package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.util.IncreasingTime;
import org.vertexium.mutation.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;

import java.util.Iterator;
import java.util.List;

public abstract class InMemoryElement<TElement extends InMemoryElement> implements Element {
    private final String id;
    private InMemoryGraph graph;
    protected final InMemoryTableElement<TElement> inMemoryTableElement;
    private final boolean includeHidden;
    private final Long endTime;
    private final Authorizations authorizations;

    protected InMemoryElement(
            InMemoryGraph graph,
            String id,
            InMemoryTableElement<TElement> inMemoryTableElement,
            boolean includeHidden,
            Long endTime,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.includeHidden = includeHidden;
        this.endTime = endTime;
        this.authorizations = authorizations;
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return inMemoryTableElement.getVisibility();
    }

    @Override
    public long getTimestamp() {
        return inMemoryTableElement.getTimestamp();
    }

    @Override
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        deleteProperty(key, name, null, authorizations);
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        getGraph().deleteProperty(this, inMemoryTableElement, key, name, visibility, authorizations);
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        for (Property p : getProperties(name)) {
            deleteProperty(p.getKey(), p.getName(), p.getVisibility(), authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        Property property = getProperty(key, name);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, null, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        softDeleteProperty(key, name, null, visibility, authorizations);
    }

    public void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, timestamp, authorizations);
        }
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(name);
        for (Property property : properties) {
            getGraph().softDeleteProperty(this, property, null, authorizations);
        }
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property.getKey(), property.getName(), property.getVisibility(), timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property, null, visibility, authorizations);
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, inMemoryTableElement, key, name, propertyVisibility, timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, inMemoryTableElement, key, name, propertyVisibility, null, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, property.getKey(), property.getName(), property.getVisibility(), timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, key, name, propertyVisibility, null, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, key, name, propertyVisibility, timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(property, null, visibility, authorizations);
    }

    @Override
    public Iterable<Object> getPropertyValues(String name) {
        return new ConvertingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property o) {
                return o.getValue();
            }
        };
    }

    @Override
    public Iterable<Object> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property o) {
                return o.getValue();
            }
        };
    }

    @Override
    public Object getPropertyValue(String name) {
        Property p = getProperty(name);
        return p == null ? null : p.getValue();
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        Property p = getProperty(key, name);
        return p == null ? null : p.getValue();
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        Iterator<Object> values = getPropertyValues(name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        Iterator<Object> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

    @Override
    public void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations) {
        addPropertyValue(ElementMutation.DEFAULT_KEY, name, value, visibility, authorizations);
    }

    @Override
    public void setProperty(String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        addPropertyValue(ElementMutation.DEFAULT_KEY, name, value, metadata, visibility, authorizations);
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations) {
        addPropertyValue(key, name, value, null, visibility, authorizations);
    }

    @Override
    public Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        for (Property p : getProperties()) {
            if (!p.getKey().equals(key)) {
                continue;
            }
            if (!p.getName().equals(name)) {
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
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public Property getProperty(String name) {
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    @Override
    public Iterable<Property> getProperties(final String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(final String key, final String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name) && property.getKey().equals(key);
            }
        };
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        addPropertyValue(key, name, value, metadata, visibility, null, true, authorizations);
    }

    public void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Long timestamp, boolean indexAfterAdd, Authorizations authorizations) {
        getGraph().addPropertyValue(this, inMemoryTableElement, key, name, value, metadata, visibility, timestamp, authorizations);
        if (indexAfterAdd) {
            getGraph().getSearchIndex().addElement(getGraph(), this, authorizations);
        }
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        return inMemoryTableElement.isHidden(authorizations);
    }

    @Override
    public Iterable<Property> getProperties() {
        return inMemoryTableElement.getProperties(includeHidden, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return inMemoryTableElement.getHistoricalPropertyValues(key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        return getHistoricalPropertyValues(key, name, visibility, null, null, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public void mergeProperties(Element element) {
        // since the backing store is shared there is no need to do this
    }

    @Override
    public Authorizations getAuthorizations() {
        return this.authorizations;
    }

    @Override
    public InMemoryGraph getGraph() {
        return this.graph;
    }

    void updatePropertiesInternal(VertexBuilder edgeBuilder) {
        updatePropertiesInternal(
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes()
        );
    }

    void updatePropertiesInternal(EdgeBuilderBase edgeBuilder) {
        updatePropertiesInternal(
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes()
        );
    }

    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations
    ) {
        long timestamp = IncreasingTime.currentTimeMillis();
        for (Property property : properties) {
            Object propertyValue = property.getValue();
            if (propertyValue instanceof StreamingPropertyValue) {
                ((MutableProperty) property).setValue(InMemoryStreamingPropertyValue.saveStreamingPropertyValue(propertyValue));
            }
            addPropertyValue(property.getKey(), property.getName(), property.getValue(), property.getMetadata(), property.getVisibility(), property.getTimestamp(), false, authorizations);
        }
        for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
            deleteProperty(propertyDeleteMutation.getKey(), propertyDeleteMutation.getName(), propertyDeleteMutation.getVisibility(), authorizations);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
            softDeleteProperty(propertySoftDeleteMutation.getKey(), propertySoftDeleteMutation.getName(), timestamp, propertySoftDeleteMutation.getVisibility(), authorizations);
        }
    }

    protected <T extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<T> mutation, Authorizations authorizations) {
        if (mutation.getElement() != this) {
            throw new VertexiumException("cannot save mutation from another element");
        }

        Iterable<Property> properties = mutation.getProperties();
        Iterable<PropertyDeleteMutation> propertyDeleteMutations = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = mutation.getPropertySoftDeletes();
        updatePropertiesInternal(
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations
        );

        long timestamp = IncreasingTime.currentTimeMillis();
        InMemoryGraph graph = getGraph();

        if (mutation.getNewElementVisibility() != null) {
            graph.alterElementVisibility(inMemoryTableElement, mutation.getNewElementVisibility());
        }

        graph.alterElementPropertyMetadata(inMemoryTableElement, mutation.getSetPropertyMetadatas(), authorizations);
        graph.alterElementPropertyVisibilities(inMemoryTableElement, mutation.getAlterPropertyVisibilities(), timestamp, authorizations);

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) (ElementMutation) mutation;
            if (edgeMutation.getNewEdgeLabel() != null) {
                graph.alterEdgeLabel((InMemoryTableEdge) inMemoryTableElement, edgeMutation.getNewEdgeLabel());
            }
        }
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
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }


    protected void saveMutationToSearchIndex(Element element, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        if (alterPropertyVisibilities != null && alterPropertyVisibilities.size() > 0) {
            for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
                Visibility existingVisibility = apv.getExistingVisibility();
                getGraph().getSearchIndex().deleteProperty(getGraph(), element, apv.getKey(), apv.getName(), existingVisibility, authorizations);
            }
            getGraph().getSearchIndex().flush();
        }
        getGraph().getSearchIndex().addElement(getGraph(), element, authorizations);
    }

    public boolean canRead(Authorizations authorizations) {
        return inMemoryTableElement.canRead(authorizations);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return inMemoryTableElement.getHiddenVisibilities();
    }
}
