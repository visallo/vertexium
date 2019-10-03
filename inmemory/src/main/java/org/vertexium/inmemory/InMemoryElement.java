package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.*;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;

import java.util.List;
import java.util.stream.Stream;

public abstract class InMemoryElement<TElement extends InMemoryElement> extends ElementBase {
    private final String id;
    private final FetchHints fetchHints;
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
    @Deprecated
    public void softDeleteProperty(String key, String name, Visibility visibility, Object eventData, Authorizations authorizations) {
        softDeleteProperty(key, name, null, visibility, eventData, IndexHint.INDEX, authorizations);
    }

    protected void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, Object data, IndexHint indexHint, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, timestamp, data, indexHint, authorizations);
        }
    }

    protected void addAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        Authorizations authorizations
    ) {
        getGraph().addAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, authorizations);
    }

    protected void deleteAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        Authorizations authorizations
    ) {
        getGraph().deleteAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, authorizations);
    }

    private void addAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility,
        Authorizations authorizations
    ) {
        getGraph().addAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility,
            authorizations
        );
    }

    private void deleteAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility,
        Authorizations authorizations
    ) {
        getGraph().deleteAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility,
            authorizations
        );
    }

    private void deleteExtendedData(String tableName, String row, String columnName, String key, Visibility visibility) {
        getGraph().deleteExtendedData(this, tableName, row, columnName, key, visibility, authorizations);
    }

    protected void extendedData(ExtendedDataMutation extendedData, Authorizations authorizations) {
        ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
            ElementType.getTypeFromElement(this),
            getId(),
            extendedData.getTableName(),
            extendedData.getRow()
        );
        getGraph().extendedData(this, extendedDataRowId, extendedData, authorizations);
    }

    @Override
    @Deprecated
    public void markPropertyHidden(
        Property property,
        Long timestamp,
        Visibility visibility,
        Object data,
        Authorizations authorizations
    ) {
        getGraph().markPropertyHidden(
            this,
            inMemoryTableElement,
            property,
            null,
            visibility,
            data,
            authorizations
        );
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
            getGraph().getSearchIndex().addElement(getGraph(), this, null, null, authorizations);
        }
    }

    @Override
    @Deprecated
    public void markPropertyVisible(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData,
        Authorizations authorizations
    ) {
        getGraph().markPropertyVisible(
            this,
            inMemoryTableElement,
            key,
            name,
            propertyVisibility,
            timestamp,
            visibility,
            eventData,
            authorizations
        );
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
    @SuppressWarnings("deprecation")
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return inMemoryTableElement.getHistoricalPropertyValues(key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints historicalEventsFetchHints,
        Authorizations authorizations
    ) {
        return inMemoryTableElement.getHistoricalEvents(getGraph(), after, historicalEventsFetchHints, authorizations);
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
            vertexBuilder.getMarkPropertyHiddenMutations(),
            vertexBuilder.getMarkPropertyVisibleMutations(),
            vertexBuilder.getAdditionalVisibilities(),
            vertexBuilder.getAdditionalVisibilityDeletes(),
            vertexBuilder.getExtendedData(),
            vertexBuilder.getExtendedDataDeletes(),
            vertexBuilder.getAdditionalExtendedDataVisibilities(),
            vertexBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            vertexBuilder.getIndexHint()
        );
    }

    void updatePropertiesInternal(EdgeBuilderBase edgeBuilder) {
        updatePropertiesInternal(
            edgeBuilder.getProperties(),
            edgeBuilder.getPropertyDeletes(),
            edgeBuilder.getPropertySoftDeletes(),
            edgeBuilder.getMarkPropertyHiddenMutations(),
            edgeBuilder.getMarkPropertyVisibleMutations(),
            edgeBuilder.getAdditionalVisibilities(),
            edgeBuilder.getAdditionalVisibilityDeletes(),
            edgeBuilder.getExtendedData(),
            edgeBuilder.getExtendedDataDeletes(),
            edgeBuilder.getAdditionalExtendedDataVisibilities(),
            edgeBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            edgeBuilder.getIndexHint()
        );
    }

    @SuppressWarnings("deprecation")
    protected void updatePropertiesInternal(
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeleteMutations,
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
        List<MarkPropertyHiddenMutation> markPropertyHiddenMutations,
        List<MarkPropertyVisibleMutation> markPropertyVisibleMutations,
        Iterable<AdditionalVisibilityAddMutation> additionalVisibilities,
        Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
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
            getGraph().deleteProperty(
                this,
                this.inMemoryTableElement,
                propertyDeleteMutation.getKey(),
                propertyDeleteMutation.getName(),
                propertyDeleteMutation.getVisibility(),
                authorizations
            );
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
            softDeleteProperty(
                propertySoftDeleteMutation.getKey(),
                propertySoftDeleteMutation.getName(),
                propertySoftDeleteMutation.getTimestamp(),
                propertySoftDeleteMutation.getVisibility(),
                propertySoftDeleteMutation.getData(),
                indexHint,
                authorizations
            );
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : additionalVisibilities) {
            addAdditionalVisibility(
                additionalVisibility.getAdditionalVisibility(),
                additionalVisibility.getEventData(),
                authorizations
            );
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : additionalVisibilityDeletes) {
            deleteAdditionalVisibility(
                additionalVisibilityDelete.getAdditionalVisibility(),
                additionalVisibilityDelete.getEventData(),
                authorizations
            );
        }
        for (MarkPropertyHiddenMutation markPropertyHidden : markPropertyHiddenMutations) {
            Property property = getProperty(
                markPropertyHidden.getPropertyKey(),
                markPropertyHidden.getPropertyName(),
                markPropertyHidden.getPropertyVisibility()
            );
            getGraph().markPropertyHidden(
                this,
                this.inMemoryTableElement,
                property,
                markPropertyHidden.getTimestamp(),
                markPropertyHidden.getVisibility(),
                markPropertyHidden.getEventData(),
                authorizations
            );
        }
        for (MarkPropertyVisibleMutation markPropertyVisible : markPropertyVisibleMutations) {
            getGraph().markPropertyVisible(
                this,
                this.inMemoryTableElement,
                markPropertyVisible.getPropertyKey(),
                markPropertyVisible.getPropertyName(),
                markPropertyVisible.getPropertyVisibility(),
                markPropertyVisible.getTimestamp(),
                markPropertyVisible.getVisibility(),
                markPropertyVisible.getEventData(),
                authorizations
            );
        }
        for (ExtendedDataMutation extendedData : extendedDatas) {
            getGraph().ensurePropertyDefined(extendedData.getColumnName(), extendedData.getValue());
            extendedData(extendedData, authorizations);
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
        for (AdditionalExtendedDataVisibilityAddMutation additionalVisibility : additionalExtendedDataVisibilities) {
            addAdditionalExtendedDataVisibility(
                additionalVisibility.getTableName(),
                additionalVisibility.getRow(),
                additionalVisibility.getAdditionalVisibility(),
                authorizations
            );
        }
        for (AdditionalExtendedDataVisibilityDeleteMutation additionalVisibilityDelete : additionalExtendedDataVisibilityDeletes) {
            deleteAdditionalExtendedDataVisibility(
                additionalVisibilityDelete.getTableName(),
                additionalVisibilityDelete.getRow(),
                additionalVisibilityDelete.getAdditionalVisibility(),
                authorizations
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
            inMemoryTableElement,
            mutation.getAlterPropertyVisibilities(),
            authorizations
        );

        updatePropertiesInternal(
            mutation.getProperties(),
            mutation.getPropertyDeletes(),
            mutation.getPropertySoftDeletes(),
            mutation.getMarkPropertyHiddenMutations(),
            mutation.getMarkPropertyVisibleMutations(),
            mutation.getAdditionalVisibilities(),
            mutation.getAdditionalVisibilityDeletes(),
            mutation.getExtendedData(),
            mutation.getExtendedDataDeletes(),
            mutation.getAdditionalExtendedDataVisibilities(),
            mutation.getAdditionalExtendedDataVisibilityDeletes(),
            indexHint
        );

        InMemoryGraph graph = getGraph();

        if (mutation.getNewElementVisibility() != null) {
            graph.alterElementVisibility(inMemoryTableElement, mutation.getNewElementVisibility(), mutation.getNewElementVisibilityData());
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

    public boolean canRead(Authorizations authorizations) {
        return inMemoryTableElement.canRead(getFetchHints(), authorizations);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return inMemoryTableElement.getHiddenVisibilities();
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return inMemoryTableElement.getAdditionalVisibilities();
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
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }

        return graph.getExtendedDataTableNames(ElementType.getTypeFromElement(this), id, getFetchHints(), authorizations);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
            getGraph(),
            this,
            tableName,
            graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, fetchHints, authorizations)
        );
    }
}
