package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.ElementIterator;
import org.vertexium.mutation.*;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;

import java.io.Serializable;
import java.util.EnumSet;

public abstract class AccumuloElement extends ElementBase implements Serializable, HasTimestamp {
    private static final long serialVersionUID = 1L;
    public static final Text CF_PROPERTY = ElementIterator.CF_PROPERTY;
    public static final Text CF_PROPERTY_METADATA = ElementIterator.CF_PROPERTY_METADATA;
    public static final Text CF_PROPERTY_SOFT_DELETE = ElementIterator.CF_PROPERTY_SOFT_DELETE;
    public static final Text CF_EXTENDED_DATA = ElementIterator.CF_EXTENDED_DATA;
    public static final Value SOFT_DELETE_VALUE = ElementIterator.SOFT_DELETE_VALUE;
    public static final Value HIDDEN_VALUE = ElementIterator.HIDDEN_VALUE;
    public static final Text CF_PROPERTY_HIDDEN = ElementIterator.CF_PROPERTY_HIDDEN;
    public static final Value HIDDEN_VALUE_DELETED = ElementIterator.HIDDEN_VALUE_DELETED;
    public static final Text DELETE_ROW_COLUMN_FAMILY = ElementIterator.DELETE_ROW_COLUMN_FAMILY;
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = ElementIterator.DELETE_ROW_COLUMN_QUALIFIER;
    public static final Text CF_SOFT_DELETE = ElementIterator.CF_SOFT_DELETE;
    public static final Text CQ_SOFT_DELETE = ElementIterator.CQ_SOFT_DELETE;
    public static final Text CF_HIDDEN = ElementIterator.CF_HIDDEN;
    public static final Text CQ_HIDDEN = ElementIterator.CQ_HIDDEN;
    public static final Text METADATA_COLUMN_FAMILY = ElementIterator.METADATA_COLUMN_FAMILY;
    public static final Text METADATA_COLUMN_QUALIFIER = ElementIterator.METADATA_COLUMN_QUALIFIER;

    protected AccumuloElement(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            EnumSet<FetchHint> fetchHints,
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
                extendedDataTableNames,
                timestamp,
                fetchHints,
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

    @Override
    protected void setVisibility(Visibility visibility) {
        super.setVisibility(visibility);
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot here

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getSetPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities(
                (AccumuloElement) mutation.getElement(),
                mutation.getAlterPropertyVisibilities(),
                authorizations
        );

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
            // Flushing here is important!
            // The call to graph.saveProperties above will issue an update request to the search index.
            // If we don't ensure that it has completed first, we run the risk of it re-adding the visibility property we are about to remove.
            getGraph().flush();
            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility(), authorizations);
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;

            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
            if (newEdgeLabel != null) {
                getGraph().alterEdgeLabel((AccumuloEdge) mutation.getElement(), newEdgeLabel);
            }
        }

        ElementType elementType = ElementType.getTypeFromElement(mutation.getElement());
        getGraph().saveExtendedDataMutations(mutation.getElement().getId(), elementType, mutation.getExtendedData());
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName) {
        return new ExtendedDataQueryableIterable(
                getGraph(),
                this,
                tableName,
                getGraph().getExtendedData(ElementType.getTypeFromElement(this), getId(), tableName, getAuthorizations())
        );
    }
}
