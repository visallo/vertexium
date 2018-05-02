package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;

import java.util.List;

public abstract class InMemoryElement<TElement extends InMemoryElement> extends ElementBase {
    private final String id;
    private final FetchHints fetchHints;
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
            FetchHints fetchHints,
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

    @Override
    public Visibility getVisibility() {
        return inMemoryTableElement.getVisibility();
    }

    @Override
    public long getTimestamp() {
        return inMemoryTableElement.getTimestamp();
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        getGraph().deleteProperty(this, inMemoryTableElement, key, name, visibility, authorizations);
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
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(
                this,
                inMemoryTableElement,
                property,
                null,
                visibility,
                authorizations
        );
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, property.getKey(), property.getName(), property.getVisibility(), timestamp, visibility, authorizations);
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

    public void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Long timestamp, boolean indexAfterAdd, Authorizations authorizations) {
        getGraph().addPropertyValue(this, inMemoryTableElement, key, name, value, metadata, visibility, timestamp, authorizations);
        if (indexAfterAdd) {
            getGraph().getSearchIndex().addElement(getGraph(), this, authorizations);
        }
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, key, name, propertyVisibility, timestamp, visibility, authorizations);
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        return inMemoryTableElement.isHidden(authorizations);
    }

    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return inMemoryTableElement.getProperties(fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return inMemoryTableElement.getHistoricalPropertyValues(key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

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

    public FetchHints getFetchHints() {
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
                graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, getFetchHints(), authorizations)
        );
    }
}
