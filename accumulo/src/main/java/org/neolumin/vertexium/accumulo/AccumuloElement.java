package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.mutation.EdgeMutation;
import org.neolumin.vertexium.mutation.ExistingElementMutationImpl;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;

import java.io.Serializable;

public abstract class AccumuloElement extends ElementBase implements Serializable, HasTimestamp {
    private static final long serialVersionUID = 1L;
    public static final Text CF_HIDDEN = new Text("H");
    public static final Text CQ_HIDDEN = new Text("H");
    public static final Value HIDDEN_VALUE = new Value("".getBytes());
    public static final Text CF_PROPERTY = new Text("PROP");
    public static final Text CF_PROPERTY_HIDDEN = new Text("PROPH");
    public static final Text CF_PROPERTY_METADATA = new Text("PROPMETA");
    private final long timestamp;

    protected AccumuloElement(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyRemoveMutation> propertyRemoveMutations,
            Iterable<Visibility> hiddenVisibilities,
            Authorizations authorizations,
            long timestamp
    ) {
        super(graph, id, visibility, properties, propertyRemoveMutations, hiddenVisibilities, authorizations);
        this.timestamp = timestamp;
    }

    @Override
    public void removeProperty(String key, String name, Authorizations authorizations) {
        Property property = super.removePropertyInternal(key, name);
        if (property != null) {
            getGraph().removeProperty(this, property, authorizations);
        }
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        Iterable<Property> properties = super.removePropertyInternal(name);
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
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot here

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getSetPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyVisibilities());

        Iterable<PropertyRemoveMutation> propertyRemoves = mutation.getPropertyRemoves();
        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties, propertyRemoves);
        getGraph().saveProperties((AccumuloElement) mutation.getElement(), properties, propertyRemoves, mutation.getIndexHint(), authorizations);

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
    public long getTimestamp() {
        return timestamp;
    }
}
