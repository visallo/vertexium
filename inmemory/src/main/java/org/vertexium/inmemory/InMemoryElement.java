package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public abstract class InMemoryElement<TElement extends InMemoryElement> implements Element {
    private final String id;
    private final EnumSet<FetchHint> fetchHints;
    private Property idProperty;
    private Property edgeLabelProperty;
    private InMemoryGraph graph;
    private InMemoryTableElement<TElement> inMemoryTableElement;
    private final Long endTime;
    private final Authorizations authorizations;

    protected InMemoryElement(
            InMemoryGraph graph,
            String id,
            InMemoryTableElement<TElement> inMemoryTableElement,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.fetchHints = fetchHints;
        this.endTime = endTime;
        this.authorizations = authorizations;
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public String getId() {
        return this.id;
    }

    protected Property getIdProperty() {
        if (idProperty == null) {
            idProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ID_PROPERTY_NAME,
                    getId(),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    fetchHints
            );
        }
        return idProperty;
    }

    protected Property getEdgeLabelProperty() {
        if (edgeLabelProperty == null && this instanceof Edge) {
            String edgeLabel = ((Edge) this).getLabel();
            edgeLabelProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    Edge.LABEL_PROPERTY_NAME,
                    edgeLabel,
                    null,
                    getTimestamp(),
                    null,
                    null,
                    fetchHints
            );
        }
        return edgeLabelProperty;
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
            getGraph().softDeleteProperty(inMemoryTableElement, property, null, IndexHint.INDEX, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        softDeleteProperty(key, name, null, visibility, IndexHint.INDEX, authorizations);
    }

    protected void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, IndexHint indexHint, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, timestamp, indexHint, authorizations);
        }
    }

    private void deleteExtendedData(String tableName, String row, String columnName, String key, Visibility visibility) {
        getGraph().deleteExtendedData(this, tableName, row, columnName, key, visibility, authorizations);
    }

    protected void extendedData(
            String tableName,
            String rowId,
            String column,
            String key,
            Object value,
            long timestamp,
            Visibility visibility,
            Authorizations authorizations
    ) {
        ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
                ElementType.getTypeFromElement(this),
                getId(),
                tableName,
                rowId
        );
        getGraph().extendedData(
                this,
                extendedDataRowId,
                column,
                key,
                value,
                timestamp,
                visibility,
                authorizations
        );
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(name);
        for (Property property : properties) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, null, IndexHint.INDEX, authorizations);
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
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
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
        if (ID_PROPERTY_NAME.equals(name)) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getIdProperty());
            return result;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getEdgeLabelProperty());
            return result;
        }
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(final String key, final String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getIdProperty());
            return result;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getEdgeLabelProperty());
            return result;
        }
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
        return inMemoryTableElement.getProperties(fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Long startTime, Long endTime, Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, null, startTime, endTime, authorizations);
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

    void updatePropertiesInternal(VertexBuilder vertexBuilder) {
        updatePropertiesInternal(
                vertexBuilder.getProperties(),
                vertexBuilder.getPropertyDeletes(),
                vertexBuilder.getPropertySoftDeletes(),
                vertexBuilder.getExtendedData(),
                vertexBuilder.getExtendedDataDeletes(),
                vertexBuilder.getIndexHint()
        );
    }

    void updatePropertiesInternal(EdgeBuilderBase edgeBuilder) {
        updatePropertiesInternal(
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                edgeBuilder.getExtendedData(),
                edgeBuilder.getExtendedDataDeletes(),
                edgeBuilder.getIndexHint()
        );
    }

    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<ExtendedDataMutation> extendedDatas,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
            IndexHint indexHint
    ) {
        for (Property property : properties) {
            addPropertyValue(
                    property.getKey(),
                    property.getName(),
                    property.getValue(),
                    property.getMetadata(),
                    property.getVisibility(),
                    property.getTimestamp(),
                    false,
                    authorizations
            );
        }
        for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
            deleteProperty(propertyDeleteMutation.getKey(), propertyDeleteMutation.getName(), propertyDeleteMutation.getVisibility(), authorizations);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
            softDeleteProperty(propertySoftDeleteMutation.getKey(), propertySoftDeleteMutation.getName(), propertySoftDeleteMutation.getTimestamp(), propertySoftDeleteMutation.getVisibility(), indexHint, authorizations);
        }
        for (ExtendedDataMutation extendedData : extendedDatas) {
            getGraph().ensurePropertyDefined(extendedData.getColumnName(), extendedData.getValue());
            extendedData(
                    extendedData.getTableName(),
                    extendedData.getRow(),
                    extendedData.getColumnName(),
                    extendedData.getKey(),
                    extendedData.getValue(),
                    extendedData.getTimestamp(),
                    extendedData.getVisibility(),
                    authorizations
            );
        }
        for (ExtendedDataDeleteMutation extendedDataDelete : extendedDataDeletes) {
            deleteExtendedData(
                    extendedDataDelete.getTableName(),
                    extendedDataDelete.getRow(),
                    extendedDataDelete.getColumnName(),
                    extendedDataDelete.getKey(),
                    extendedDataDelete.getVisibility()
            );
        }
    }

    protected <T extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<T> mutation, IndexHint indexHint, Authorizations authorizations) {
        if (mutation.getElement() != this) {
            throw new VertexiumException("cannot save mutation from another element");
        }

        // Order matters a lot here

        // Metadata must be altered first because the lookup of a property can include visibility which will be
        // altered by alterElementPropertyVisibilities
        graph.alterElementPropertyMetadata(inMemoryTableElement, mutation.getSetPropertyMetadatas(), authorizations);

        // Altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        graph.alterElementPropertyVisibilities(
                inMemoryTableElement, mutation.getAlterPropertyVisibilities(), authorizations);

        Iterable<Property> properties = mutation.getProperties();
        Iterable<PropertyDeleteMutation> propertyDeleteMutations = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = mutation.getPropertySoftDeletes();

        updatePropertiesInternal(
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                mutation.getExtendedData(),
                mutation.getExtendedDataDeletes(),
                indexHint
        );

        InMemoryGraph graph = getGraph();

        if (mutation.getNewElementVisibility() != null) {
            graph.alterElementVisibility(inMemoryTableElement, mutation.getNewElementVisibility());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;
            if (edgeMutation.getNewEdgeLabel() != null) {
                graph.alterEdgeLabel((InMemoryTableEdge) inMemoryTableElement, edgeMutation.getAlterEdgeLabelTimestamp(), edgeMutation.getNewEdgeLabel());
            }
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
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


    protected void saveMutationToSearchIndex(
            Element element,
            Visibility oldVisibility,
            Visibility newVisibility,
            List<AlterPropertyVisibility> alterPropertyVisibilities,
            Iterable<ExtendedDataMutation> extendedDatas,
            Authorizations authorizations
    ) {
        if (alterPropertyVisibilities != null && alterPropertyVisibilities.size() > 0) {
            // Bulk delete
            List<PropertyDescriptor> propertyList = Lists.newArrayList();
            alterPropertyVisibilities.forEach(p -> propertyList.add(PropertyDescriptor.from(p.getKey(), p.getName(), p.getExistingVisibility())));
            getGraph().getSearchIndex().deleteProperties(
                    getGraph(),
                    element,
                    propertyList,
                    authorizations
            );

            getGraph().getSearchIndex().flush(getGraph());
        }
        if (newVisibility != null) {
            getGraph().getSearchIndex().alterElementVisibility(getGraph(), element, oldVisibility, newVisibility, authorizations);
        } else {
            getGraph().getSearchIndex().addElement(getGraph(), element, authorizations);
            getGraph().getSearchIndex().addElementExtendedData(getGraph(), element, extendedDatas, authorizations);
        }
    }

    public boolean canRead(Authorizations authorizations) {
        return inMemoryTableElement.canRead(authorizations);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return inMemoryTableElement.getHiddenVisibilities();
    }

    public EnumSet<FetchHint> getFetchHints() {
        return fetchHints;
    }

    protected InMemoryTableElement<TElement> getInMemoryTableElement() {
        return inMemoryTableElement;
    }

    protected void setInMemoryTableElement(InMemoryTableElement<TElement> inMemoryTableElement) {
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        return graph.getExtendedDataTableNames(ElementType.getTypeFromElement(this), id, authorizations);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName) {
        return new ExtendedDataQueryableIterable(
                getGraph(),
                this,
                tableName,
                graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, authorizations)
        );
    }
}
