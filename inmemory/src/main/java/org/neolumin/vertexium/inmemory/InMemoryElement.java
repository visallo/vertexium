package org.neolumin.vertexium.inmemory;

import org.neolumin.vertexium.*;
import org.neolumin.vertexium.mutation.EdgeMutation;
import org.neolumin.vertexium.mutation.ExistingElementMutationImpl;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;
import org.neolumin.vertexium.property.MutableProperty;
import org.neolumin.vertexium.property.StreamingPropertyValue;
import org.neolumin.vertexium.util.StreamUtils;

import java.io.IOException;

public abstract class InMemoryElement extends ElementBase {
    protected InMemoryElement(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyRemoveMutation> propertyRemoveMutations,
            Iterable<Visibility> hiddenVisibilities,
            Authorizations authorizations
    ) {
        super(graph, id, visibility, properties, propertyRemoveMutations, hiddenVisibilities, authorizations);
    }

    @Override
    public void removeProperty(String key, String name, Authorizations authorizations) {
        Property property = removePropertyInternal(key, name);
        if (property != null) {
            getGraph().removeProperty(this, property, authorizations);
        }
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        Iterable<Property> properties = removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().removeProperty(this, property, authorizations);
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
    protected void updatePropertiesInternal(Iterable<Property> properties, Iterable<PropertyRemoveMutation> propertyRemoveMutations) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream());
                    ((MutableProperty) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.updatePropertiesInternal(properties, propertyRemoveMutations);
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    @Override
    protected Iterable<Property> removePropertyInternal(String name) {
        return super.removePropertyInternal(name);
    }

    @Override
    protected Property removePropertyInternal(String key, String name) {
        return super.removePropertyInternal(key, name);
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        Iterable<Property> properties = mutation.getProperties();
        Iterable<PropertyRemoveMutation> propertyRemoves = mutation.getPropertyRemoves();
        updatePropertiesInternal(properties, propertyRemoves);
        getGraph().saveProperties(mutation.getElement(), properties, propertyRemoves, mutation.getIndexHint(), authorizations);

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
        if (getVisibility().getVisibilityString().length() > 0 && !authorizations.canRead(getVisibility())) {
            return false;
        }

        return true;
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
        updatePropertiesInternal(newVertex.getProperties(), newVertex.getPropertyRemoveMutations());
    }
}
