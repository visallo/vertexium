package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.mutation.EdgeMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.StreamUtils;

import java.io.IOException;

public abstract class InMemoryElement extends ElementBase {
    private InMemoryHistoricalPropertyValues historicalPropertyValues;
    private long startTime;

    protected InMemoryElement(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            InMemoryHistoricalPropertyValues historicalPropertyValues,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long startTime,
            long timestamp,
            Authorizations authorizations
    ) {
        super(graph, id, visibility, properties, propertyDeleteMutations, propertySoftDeleteMutations, hiddenVisibilities, timestamp, authorizations);
        this.startTime = startTime;
        if (this.historicalPropertyValues == null) {
            this.historicalPropertyValues = new InMemoryHistoricalPropertyValues();
        }
        this.historicalPropertyValues.update(historicalPropertyValues);
    }

    public Property removePropertyInternal(String key, String name) {
        return super.removePropertyInternal(key, name);
    }

    public Property softDeletePropertyInternal(String key, String name) {
        return super.softDeletePropertyInternal(key, name);
    }

    public Iterable<Property> softDeletePropertyInternal(String name) {
        return super.softDeletePropertyInternal(name);
    }

    @Override
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        Property property = removePropertyInternal(key, name);
        if (property != null) {
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        Property property = softDeletePropertyInternal(key, name);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = softDeletePropertyInternal(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = softDeletePropertyInternal(name);
        for (Property property : properties) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, property, visibility, authorizations);
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations
    ) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream());
                    ((MutableProperty) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        Iterable<Property> properties = mutation.getProperties();
        Iterable<PropertyDeleteMutation> propertyDeleteMutations = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = mutation.getPropertySoftDeletes();
        updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
        getGraph().saveProperties(
                mutation.getElement(),
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                mutation.getIndexHint(),
                authorizations
        );

        if (mutation.getElement() instanceof Edge) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterEdgeVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterEdgePropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities(), authorizations);
            getGraph().alterEdgePropertyMetadata(mutation.getElement().getId(), mutation.getSetPropertyMetadatas());
            if (mutation instanceof EdgeMutation) {
                EdgeMutation edgeMutation = (EdgeMutation) mutation;
                if (edgeMutation.getNewEdgeLabel() != null) {
                    getGraph().alterEdgeLabel(mutation.getElement().getId(), edgeMutation.getNewEdgeLabel());
                }
            }
        } else if (mutation.getElement() instanceof Vertex) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterVertexVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterVertexPropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities(), authorizations);
            getGraph().alterVertexPropertyMetadata(mutation.getElement().getId(), mutation.getSetPropertyMetadatas());
        } else {
            throw new IllegalStateException("Unexpected element type: " + mutation.getElement());
        }
    }

    void setVisibilityInternal(Visibility visibility) {
        super.setVisibility(visibility);
    }

    public void addHiddenVisibility(Visibility visibility) {
        super.addHiddenVisibility(visibility);
    }

    public void removeHiddenVisibility(Visibility visibility) {
        super.removeHiddenVisibility(visibility);
    }

    public boolean canRead(Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        //noinspection SimplifiableIfStatement
        if (getVisibility().getVisibilityString().length() == 0) {
            return true;
        }

        return authorizations.canRead(getVisibility());
    }

    void markPropertyHiddenInternal(Property property, Visibility visibility) {
        if (property instanceof MutableProperty) {
            ((MutableProperty) property).addHiddenVisibility(visibility);
        } else {
            throw new VertexiumException("Could not mark property hidden. Must be of type " + MutableProperty.class.getName());
        }
    }

    void markPropertyVisibleInternal(Property property, Visibility visibility) {
        if (property instanceof MutableProperty) {
            ((MutableProperty) property).removeHiddenVisibility(visibility);
        } else {
            throw new VertexiumException("Could not mark property visible. Must be of type " + MutableProperty.class.getName());
        }
    }

    protected void updateExisting(InMemoryVertex newVertex) {
        updatePropertiesInternal(
                newVertex.getProperties(),
                newVertex.getPropertyDeleteMutations(),
                newVertex.getPropertySoftDeleteMutations()
        );
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility);
    }

    public InMemoryHistoricalPropertyValues getHistoricalPropertyValues() {
        return historicalPropertyValues;
    }

    @Override
    protected void addPropertyInternal(Property property) {
        Object propertyValue = property.getValue();
        if (!(propertyValue instanceof PropertyValue) || ((PropertyValue) propertyValue).isStore()) {
            if (historicalPropertyValues == null) {
                historicalPropertyValues = new InMemoryHistoricalPropertyValues();
            }
            historicalPropertyValues.addProperty(property);
        }

        super.addPropertyInternal(property);
    }

    public Iterable<HistoricalPropertyValue> internalGetHistoricalPropertyValues(String propertyKey, String propertyName, Visibility propertyVisibility) {
        if (historicalPropertyValues == null) {
            historicalPropertyValues = new InMemoryHistoricalPropertyValues();
        }
        return historicalPropertyValues.get(propertyKey, propertyName, propertyVisibility);
    }

    public long getStartTime() {
        return startTime;
    }
}
