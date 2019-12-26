package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.ElementIterator;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;
import org.vertexium.util.PropertyCollection;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

public abstract class AccumuloElement extends ElementBase implements Serializable, HasTimestamp {
    private static final long serialVersionUID = 1L;
    public static final Text CF_PROPERTY = ElementIterator.CF_PROPERTY;
    public static final Text CF_PROPERTY_METADATA = ElementIterator.CF_PROPERTY_METADATA;
    public static final Text CF_PROPERTY_SOFT_DELETE = ElementIterator.CF_PROPERTY_SOFT_DELETE;
    public static final Text CF_EXTENDED_DATA = ElementIterator.CF_EXTENDED_DATA;
    public static final Value SOFT_DELETE_VALUE = ElementIterator.SOFT_DELETE_VALUE;
    public static final Value SOFT_DELETE_VALUE_DELETED = ElementIterator.SOFT_DELETE_VALUE_DELETED;
    public static final Value HIDDEN_VALUE = ElementIterator.HIDDEN_VALUE;
    public static final Text CF_PROPERTY_HIDDEN = ElementIterator.CF_PROPERTY_HIDDEN;
    public static final Value HIDDEN_VALUE_DELETED = ElementIterator.HIDDEN_VALUE_DELETED;
    public static final Value ADD_ADDITIONAL_VISIBILITY_VALUE = ElementIterator.ADDITIONAL_VISIBILITY_VALUE;
    public static final Value ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED = ElementIterator.ADDITIONAL_VISIBILITY_VALUE_DELETED;
    public static final Value SIGNAL_VALUE_DELETED = ElementIterator.SIGNAL_VALUE_DELETED;
    public static final Text DELETE_ROW_COLUMN_FAMILY = ElementIterator.DELETE_ROW_COLUMN_FAMILY;
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = ElementIterator.DELETE_ROW_COLUMN_QUALIFIER;
    public static final Text CF_SOFT_DELETE = ElementIterator.CF_SOFT_DELETE;
    public static final Text CQ_SOFT_DELETE = ElementIterator.CQ_SOFT_DELETE;
    public static final Text CF_HIDDEN = ElementIterator.CF_HIDDEN;
    public static final Text CQ_HIDDEN = ElementIterator.CQ_HIDDEN;
    public static final Text CF_ADDITIONAL_VISIBILITY = ElementIterator.CF_ADDITIONAL_VISIBILITY;
    public static final Text METADATA_COLUMN_FAMILY = ElementIterator.METADATA_COLUMN_FAMILY;
    public static final Text METADATA_COLUMN_QUALIFIER = ElementIterator.METADATA_COLUMN_QUALIFIER;

    private final Graph graph;
    private final String id;
    private Visibility visibility;
    private final long timestamp;
    private final FetchHints fetchHints;
    private final Set<Visibility> hiddenVisibilities;
    private final Set<String> additionalVisibilities;

    private final PropertyCollection properties;
    private final ImmutableSet<String> extendedDataTableNames;
    private ConcurrentSkipListSet<PropertyDeleteMutation> propertyDeleteMutations;
    private ConcurrentSkipListSet<PropertySoftDeleteMutation> propertySoftDeleteMutations;
    private final Authorizations authorizations;

    protected AccumuloElement(
        Graph graph,
        String id,
        Visibility visibility,
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeleteMutations,
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
        Iterable<Visibility> hiddenVisibilities,
        Iterable<String> additionalVisibilities,
        ImmutableSet<String> extendedDataTableNames,
        long timestamp,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
        this.timestamp = timestamp;
        this.fetchHints = fetchHints;
        this.properties = new PropertyCollection();
        this.extendedDataTableNames = extendedDataTableNames;
        this.authorizations = authorizations;

        ImmutableSet.Builder<Visibility> hiddenVisibilityBuilder = new ImmutableSet.Builder<>();
        if (hiddenVisibilities != null) {
            for (Visibility v : hiddenVisibilities) {
                hiddenVisibilityBuilder.add(v);
            }
        }
        this.hiddenVisibilities = hiddenVisibilityBuilder.build();
        this.additionalVisibilities = Sets.newHashSet(additionalVisibilities);
        updatePropertiesInternal(
            properties,
            propertyDeleteMutations,
            propertySoftDeleteMutations,
            null,
            null
        );
    }

    @Override
    @Deprecated
    public void softDeleteProperty(String key, String name, Visibility visibility, Object eventData, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
            getGraph().softDeleteProperty(this, property, eventData, authorizations);
        }
    }

    @Override
    @Deprecated
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Object data, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, timestamp, visibility, data, authorizations);
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) graph;
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot in this method
        AccumuloElement element = (AccumuloElement) mutation.getElement();

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas(element, mutation.getSetPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities(element, mutation.getAlterPropertyVisibilities());

        Iterable<PropertyDeleteMutation> propertyDeletes = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeletes = mutation.getPropertySoftDeletes();
        Iterable<Property> properties = mutation.getProperties();
        Iterable<AdditionalVisibilityAddMutation> additionalVisibilities = mutation.getAdditionalVisibilities();
        Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes = mutation.getAdditionalVisibilityDeletes();
        Iterable<MarkPropertyHiddenMutation> markPropertyHiddenMutations = mutation.getMarkPropertyHiddenMutations();
        Iterable<MarkPropertyVisibleMutation> markPropertyVisibleMutations = mutation.getMarkPropertyVisibleMutations();

        updatePropertiesInternal(
            properties,
            propertyDeletes,
            propertySoftDeletes,
            markPropertyHiddenMutations,
            markPropertyVisibleMutations
        );
        updateAdditionalVisibilitiesInternal(additionalVisibilities, additionalVisibilityDeletes);
        getGraph().savePropertiesAndAdditionalVisibilities(
            element,
            properties,
            propertyDeletes,
            propertySoftDeletes,
            additionalVisibilities,
            additionalVisibilityDeletes,
            markPropertyHiddenMutations,
            markPropertyVisibleMutations
        );

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility(element, mutation.getNewElementVisibility(), mutation.getNewElementVisibilityData());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;

            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
            if (newEdgeLabel != null) {
                getGraph().alterEdgeLabel((AccumuloEdge) mutation.getElement(), newEdgeLabel);
            }
        }

        if (mutation.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getGraph().getSearchIndex().updateElement(graph, mutation, authorizations);
        }

        ElementType elementType = ElementType.getTypeFromElement(mutation.getElement());
        getGraph().saveExtendedDataMutations(
            mutation.getElement(),
            elementType,
            mutation.getIndexHint(),
            mutation.getExtendedData(),
            mutation.getExtendedDataDeletes(),
            mutation.getAdditionalExtendedDataVisibilities(),
            mutation.getAdditionalExtendedDataVisibilityDeletes(),
            authorizations
        );
    }

    @Override
    @SuppressWarnings("deprecation")
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        return getGraph().getHistoricalEvents(
            Lists.newArrayList(this),
            after,
            fetchHints,
            authorizations
        );
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
            getGraph(),
            this,
            tableName,
            getGraph().getExtendedData(
                ElementType.getTypeFromElement(this),
                getId(),
                tableName,
                fetchHints,
                getAuthorizations()
            )
        );
    }

    @Override
    public Property getProperty(String key, String name) {
        return getProperty(key, name, null);
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
    public Object getPropertyValue(String name, int index) {
        return getPropertyValue(null, name, index);
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return reservedProperty.getValue();
        }
        getFetchHints().assertPropertyIncluded(name);
        Property property = this.properties.getProperty(key, name, index);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    @Override
    public String getId() {
        return this.id;
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
        if (!getFetchHints().isIncludeProperties()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return this.properties.getProperties();
    }

    public Iterable<PropertyDeleteMutation> getPropertyDeleteMutations() {
        return this.propertyDeleteMutations;
    }

    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeleteMutations() {
        return this.propertySoftDeleteMutations;
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return Lists.newArrayList(reservedProperty);
        }
        getFetchHints().assertPropertyIncluded(name);
        return this.properties.getProperties(key, name);
    }

    private void updateAdditionalVisibilitiesInternal(Iterable<AdditionalVisibilityAddMutation> additionalVisibilities, Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes) {
        if (additionalVisibilities != null) {
            for (AdditionalVisibilityAddMutation additionalVisibility : additionalVisibilities) {
                this.additionalVisibilities.add(additionalVisibility.getAdditionalVisibility());
            }
        }
        if (additionalVisibilityDeletes != null) {
            for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : additionalVisibilityDeletes) {
                this.additionalVisibilities.remove(additionalVisibilityDelete.getAdditionalVisibility());
            }
        }
    }

    // this method differs setProperties in that it only updates the in memory representation of the properties
    protected void updatePropertiesInternal(
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeleteMutations,
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
        Iterable<MarkPropertyHiddenMutation> markPropertyHiddenMutations,
        Iterable<MarkPropertyVisibleMutation> markPropertyVisibleMutations
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

        if (markPropertyHiddenMutations != null) {
            for (MarkPropertyHiddenMutation markPropertyHiddenMutation : markPropertyHiddenMutations) {
                markPropertyHiddenInternal(markPropertyHiddenMutation);
            }
        }

        if (markPropertyVisibleMutations != null) {
            for (MarkPropertyVisibleMutation markPropertyVisibleMutation : markPropertyVisibleMutations) {
                markPropertyVisibleInternal(markPropertyVisibleMutation);
            }
        }
    }

    private void markPropertyVisibleInternal(MarkPropertyVisibleMutation mutation) {
        Property property = getProperty(
            mutation.getPropertyKey(),
            mutation.getPropertyName(),
            mutation.getPropertyVisibility()
        );
        if (property instanceof MutableProperty) {
            ((MutableProperty) property).removeHiddenVisibility(mutation.getVisibility());
        }
    }

    private void markPropertyHiddenInternal(MarkPropertyHiddenMutation mutation) {
        Property property = getProperty(
            mutation.getPropertyKey(),
            mutation.getPropertyName(),
            mutation.getPropertyVisibility()
        );
        if (property instanceof MutableProperty) {
            ((MutableProperty) property).addHiddenVisibility(mutation.getVisibility());
        }
    }

    protected void removePropertyInternal(String key, String name, Visibility visibility) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
        }
    }

    protected void addPropertyInternal(Property property) {
        if (property.getKey() == null) {
            throw new IllegalArgumentException("key is required for property");
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

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return ImmutableSet.copyOf(additionalVisibilities);
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }

        return extendedDataTableNames;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    protected Iterable<Property> internalGetProperties(String key, String name) {
        getFetchHints().assertPropertyIncluded(name);
        return this.properties.getProperties(key, name);
    }
}
