package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.ElementIterator;
import org.vertexium.mutation.EdgeMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.MutableProperty;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;
import org.vertexium.util.PropertyCollection;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

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

    private final Graph graph;
    private final String id;
    private Visibility visibility;
    private final long timestamp;
    private final FetchHints fetchHints;
    private final Set<Visibility> hiddenVisibilities;

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
        updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
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

        updatePropertiesInternal(properties, propertyDeletes, propertySoftDeletes);
        getGraph().saveProperties(element, properties, propertyDeletes, propertySoftDeletes);

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility(element, mutation.getNewElementVisibility());
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
                authorizations
        );
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
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getOutVertexIdProperty();
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInVertexIdProperty();
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInOrOutVertexIdProperty();
        }
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getOutVertexIdProperty();
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInVertexIdProperty();
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInOrOutVertexIdProperty();
        }
        getFetchHints().assertPropertyIncluded(name);
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
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getOutVertexIdProperty();
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInVertexIdProperty();
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInOrOutVertexIdProperty();
        }
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
    public Iterable<Property> getProperties(final String key, final String name) {
        if (ID_PROPERTY_NAME.equals(name)
                || (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge)
                || (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)
                || (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)
                || (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)) {
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
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
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
