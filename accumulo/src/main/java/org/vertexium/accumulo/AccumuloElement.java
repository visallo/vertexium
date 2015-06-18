package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.mutation.EdgeMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.io.Serializable;

public abstract class AccumuloElement extends ElementBase implements Serializable, HasTimestamp {
    private static final long serialVersionUID = 1L;
    public static final String CF_HIDDEN_STRING = "H";
    public static final Text CF_HIDDEN = new Text(CF_HIDDEN_STRING);
    public static final Text CQ_HIDDEN = new Text("H");
    public static final String CF_SOFT_DELETE_STRING = "D";
    public static final Text CF_SOFT_DELETE = new Text(CF_SOFT_DELETE_STRING);
    public static final Text CQ_SOFT_DELETE = new Text("D");
    public static final Value HIDDEN_VALUE = new Value("".getBytes());
    public static final Value HIDDEN_VALUE_DELETED = new Value("X".getBytes());
    public static final Value SOFT_DELETE_VALUE = new Value("".getBytes());
    public static final String CF_PROPERTY_STRING = "PROP";
    public static final Text CF_PROPERTY = new Text(CF_PROPERTY_STRING);
    public static final String CF_PROPERTY_HIDDEN_STRING = "PROPH";
    public static final Text CF_PROPERTY_HIDDEN = new Text(CF_PROPERTY_HIDDEN_STRING);
    public static final String CF_PROPERTY_SOFT_DELETE_STRING = "PROPD";
    public static final Text CF_PROPERTY_SOFT_DELETE = new Text(CF_PROPERTY_SOFT_DELETE_STRING);
    public static final String CF_PROPERTY_METADATA_STRING = "PROPMETA";
    public static final Text CF_PROPERTY_METADATA = new Text(CF_PROPERTY_METADATA_STRING);

    protected AccumuloElement(
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
        super(
                graph,
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                authorizations
        );
    }

    @Override
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        Property property = super.removePropertyInternal(key, name);
        if (property != null) {
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = super.removePropertyInternal(key, name, visibility);
        if (property != null) {
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        Property property = super.softDeletePropertyInternal(key, name);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = super.softDeletePropertyInternal(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = super.removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = super.removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, property, timestamp, visibility, authorizations);
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot here

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getSetPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyVisibilities());

        Iterable<PropertyDeleteMutation> propertyDeletes = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeletes = mutation.getPropertySoftDeletes();
        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties, propertyDeletes, propertySoftDeletes);
        getGraph().saveProperties(
                (AccumuloElement) mutation.getElement(),
                properties,
                propertyDeletes,
                propertySoftDeletes,
                mutation.getIndexHint(),
                authorizations
        );

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;

            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
            if (newEdgeLabel != null) {
                getGraph().alterEdgeLabel((AccumuloEdge) mutation.getElement(), newEdgeLabel);
            }
        }
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility, startTime, endTime, authorizations);
    }
}
