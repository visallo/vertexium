package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.*;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;
import org.vertexium.search.IndexHint;
import org.vertexium.util.FutureDeprecation;

import java.util.stream.Stream;

public abstract class InMemoryElement<TElement extends InMemoryElement> extends ElementBase {
    private final String id;
    private final FetchHints fetchHints;
    private InMemoryGraph graph;
    private InMemoryTableElement<TElement> inMemoryTableElement;
    private final Long endTime;
    private final User user;

    protected InMemoryElement(
        InMemoryGraph graph,
        String id,
        InMemoryTableElement<TElement> inMemoryTableElement,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        this.graph = graph;
        this.id = id;
        this.fetchHints = fetchHints;
        this.endTime = endTime;
        this.user = user;
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
    public User getUser() {
        return user;
    }

    protected void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, Object data, IndexHint indexHint, User user) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, timestamp, data, indexHint, user);
        }
    }

    protected void addAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        User user
    ) {
        getGraph().addAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, user);
    }

    protected void deleteAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        User user
    ) {
        getGraph().deleteAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, user);
    }

    private void addAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility
    ) {
        getGraph().addAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility
        );
    }

    private void deleteAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility
    ) {
        getGraph().deleteAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility
        );
    }

    private void deleteExtendedData(String tableName, String row, String columnName, String key, Visibility visibility) {
        getGraph().deleteExtendedData(this, tableName, row, columnName, key, visibility, user);
    }

    protected void extendedData(ExtendedDataMutation extendedData, User user) {
        ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
            ElementType.getTypeFromElement(this),
            getId(),
            extendedData.getTableName(),
            extendedData.getRow()
        );
        getGraph().extendedData(this, extendedDataRowId, extendedData, user);
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

    public void addPropertyValue(
        String key,
        String name,
        Object value,
        Metadata metadata,
        Visibility visibility,
        Long timestamp,
        boolean indexAfterAdd,
        User user
    ) {
        getGraph().addPropertyValue(this, inMemoryTableElement, key, name, value, metadata, visibility, timestamp, user);
        if (indexAfterAdd) {
            getGraph().getSearchIndex().addElement(getGraph(), this, null, null, user);
        }
    }

    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return inMemoryTableElement.getProperties(fetchHints, endTime, user);
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints historicalEventsFetchHints,
        User user
    ) {
        return inMemoryTableElement.getHistoricalEvents(getGraph(), after, historicalEventsFetchHints, user);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public InMemoryGraph getGraph() {
        return this.graph;
    }

    void updateInternal(VertexBuilder vertexBuilder) {
        updateInternal(
            vertexBuilder.isDeleteElement(),
            vertexBuilder.getSoftDeleteData(),
            vertexBuilder.getMarkHiddenData(),
            vertexBuilder.getMarkVisibleData(),
            vertexBuilder.getProperties(),
            vertexBuilder.getPropertyDeletes(),
            vertexBuilder.getPropertySoftDeletes(),
            vertexBuilder.getMarkPropertyHiddenData(),
            vertexBuilder.getMarkPropertyVisibleData(),
            vertexBuilder.getAdditionalVisibilities(),
            vertexBuilder.getAdditionalVisibilityDeletes(),
            vertexBuilder.getExtendedData(),
            vertexBuilder.getDeleteExtendedDataRowData(),
            vertexBuilder.getExtendedDataDeletes(),
            vertexBuilder.getAdditionalExtendedDataVisibilities(),
            vertexBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            vertexBuilder.getIndexHint()
        );
    }

    void updateInternal(EdgeBuilderBase edgeBuilder) {
        updateInternal(
            edgeBuilder.isDeleteElement(),
            edgeBuilder.getSoftDeleteData(),
            edgeBuilder.getMarkHiddenData(),
            edgeBuilder.getMarkVisibleData(),
            edgeBuilder.getProperties(),
            edgeBuilder.getPropertyDeletes(),
            edgeBuilder.getPropertySoftDeletes(),
            edgeBuilder.getMarkPropertyHiddenData(),
            edgeBuilder.getMarkPropertyVisibleData(),
            edgeBuilder.getAdditionalVisibilities(),
            edgeBuilder.getAdditionalVisibilityDeletes(),
            edgeBuilder.getExtendedData(),
            edgeBuilder.getDeleteExtendedDataRowData(),
            edgeBuilder.getExtendedDataDeletes(),
            edgeBuilder.getAdditionalExtendedDataVisibilities(),
            edgeBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            edgeBuilder.getIndexHint()
        );
    }

    protected void updateInternal(
        boolean deleteElement,
        ElementMutationBase.SoftDeleteData softDeleteData,
        Iterable<ElementMutationBase.MarkHiddenData> markHiddenData,
        Iterable<ElementMutationBase.MarkVisibleData> markVisibleData,
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeleteMutations,
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
        Iterable<ElementMutationBase.MarkPropertyHiddenData> markPropertyHiddenData,
        Iterable<ElementMutationBase.MarkPropertyVisibleData> markPropertyVisibleData,
        Iterable<AdditionalVisibilityAddMutation> additionalVisibilities,
        Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<ElementMutationBase.DeleteExtendedDataRowData> deleteExtendedDataRowData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
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
                user
            );
        }

        for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
            graph.deleteProperty(
                this,
                inMemoryTableElement,
                propertyDeleteMutation.getKey(),
                propertyDeleteMutation.getName(),
                propertyDeleteMutation.getVisibility(),
                user
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
                user
            );
        }

        for (AdditionalVisibilityAddMutation additionalVisibility : additionalVisibilities) {
            addAdditionalVisibility(
                additionalVisibility.getAdditionalVisibility(),
                additionalVisibility.getEventData(),
                user
            );
        }

        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : additionalVisibilityDeletes) {
            deleteAdditionalVisibility(
                additionalVisibilityDelete.getAdditionalVisibility(),
                additionalVisibilityDelete.getEventData(),
                user
            );
        }

        for (ElementMutationBase.DeleteExtendedDataRowData data : deleteExtendedDataRowData) {
            getGraph().deleteExtendedDataRow(
                this,
                data.getTableName(),
                data.getRow(),
                user
            );
        }

        for (ExtendedDataMutation extendedData : extendedDatas) {
            getGraph().ensurePropertyDefined(extendedData.getColumnName(), extendedData.getValue());
            extendedData(extendedData, user);
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
                additionalVisibility.getAdditionalVisibility()
            );
        }

        for (AdditionalExtendedDataVisibilityDeleteMutation additionalVisibilityDelete : additionalExtendedDataVisibilityDeletes) {
            deleteAdditionalExtendedDataVisibility(
                additionalVisibilityDelete.getTableName(),
                additionalVisibilityDelete.getRow(),
                additionalVisibilityDelete.getAdditionalVisibility()
            );
        }

        for (ElementMutationBase.MarkPropertyVisibleData data : markPropertyVisibleData) {
            graph.markPropertyVisible(
                this,
                getInMemoryTableElement(),
                data.getKey(),
                data.getName(),
                data.getPropertyVisibility(),
                data.getTimestamp(),
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkPropertyHiddenData data : markPropertyHiddenData) {
            graph.markPropertyHidden(
                this,
                getInMemoryTableElement(),
                data.getKey(),
                data.getName(),
                data.getPropertyVisibility(),
                data.getTimestamp(),
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkVisibleData data : markVisibleData) {
            graph.markElementVisible(
                this,
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkHiddenData data : markHiddenData) {
            graph.markElementHidden(
                this,
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        if (softDeleteData != null) {
            graph.softDeleteElement(this, softDeleteData.getTimestamp(), softDeleteData.getEventData(), user);
        }

        if (deleteElement) {
            graph.deleteElement(this, user);
        }
    }

    protected <T extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<T> mutation, IndexHint indexHint, User user) {
        if (mutation.getElement() != this) {
            throw new VertexiumException("cannot save mutation from another element");
        }

        // Order matters a lot here

        // Metadata must be altered first because the lookup of a property can include visibility which will be
        // altered by alterElementPropertyVisibilities
        graph.alterElementPropertyMetadata(inMemoryTableElement, mutation.getSetPropertyMetadata(), user);

        // Altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        graph.alterElementPropertyVisibilities(
            inMemoryTableElement,
            mutation.getAlterPropertyVisibilities(),
            user
        );

        updateInternal(
            mutation.isDeleteElement(),
            mutation.getSoftDeleteData(),
            mutation.getMarkHiddenData(),
            mutation.getMarkVisibleData(),
            mutation.getProperties(),
            mutation.getPropertyDeletes(),
            mutation.getPropertySoftDeletes(),
            mutation.getMarkPropertyHiddenData(),
            mutation.getMarkPropertyVisibleData(),
            mutation.getAdditionalVisibilities(),
            mutation.getAdditionalVisibilityDeletes(),
            mutation.getExtendedData(),
            mutation.getDeleteExtendedDataRowData(),
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

    @FutureDeprecation
    public boolean canRead(Authorizations authorizations) {
        return canRead(authorizations.getUser());
    }

    public boolean canRead(User user) {
        return inMemoryTableElement.canRead(getFetchHints(), user);
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

        return graph.getExtendedDataTableNames(ElementType.getTypeFromElement(this), id, getFetchHints(), user);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
            getGraph(),
            this,
            tableName,
            graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, fetchHints, user)
        );
    }
}
