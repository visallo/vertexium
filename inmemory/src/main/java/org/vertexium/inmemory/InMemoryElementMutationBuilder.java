package org.vertexium.inmemory;

import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.event.*;
import org.vertexium.inmemory.mutations.AlterEdgeLabelMutation;
import org.vertexium.inmemory.mutations.AlterVisibilityMutation;
import org.vertexium.inmemory.mutations.EdgeSetupMutation;
import org.vertexium.inmemory.mutations.ElementTimestampMutation;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.IncreasingTime;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

class InMemoryElementMutationBuilder {
    private final InMemoryGraph graph;
    private final InMemoryVertexTable vertices;
    private final InMemoryEdgeTable edges;
    private final InMemoryExtendedDataTable extendedDataTable;
    private final Consumer<GraphEvent> fireGraphEvent;

    public InMemoryElementMutationBuilder(
        InMemoryGraph graph,
        InMemoryVertexTable vertices,
        InMemoryEdgeTable edges,
        InMemoryExtendedDataTable extendedDataTable,
        Consumer<GraphEvent> fireGraphEvent
    ) {
        this.graph = graph;
        this.vertices = vertices;
        this.edges = edges;
        this.extendedDataTable = extendedDataTable;
        this.fireGraphEvent = fireGraphEvent;
    }

    private void fireGraphEvent(GraphEvent graphEvent) {
        this.fireGraphEvent.accept(graphEvent);
    }

    public <T extends Element> void saveExistingElementMutation(ExistingElementMutationBase<T> mutation, User user) {
        InMemoryElement element = (InMemoryElement) mutation.getElement();
        InMemoryTableElement inMemoryTableElement = element.getInMemoryTableElement();

        // Order matters a lot here

        // Metadata must be altered first because the lookup of a property can include visibility which will be
        // altered by alterElementPropertyVisibilities
        alterElementPropertyMetadata(inMemoryTableElement, mutation.getSetPropertyMetadata(), user);

        // Altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        alterElementPropertyVisibilities(inMemoryTableElement, mutation.getAlterPropertyVisibilities(), user);

        update(mutation, element, user);

        if (mutation.getNewElementVisibility() != null) {
            alterElementVisibility(inMemoryTableElement, mutation.getNewElementVisibility(), mutation.getNewElementVisibilityData());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;
            if (edgeMutation.getNewEdgeLabel() != null) {
                alterEdgeLabel((InMemoryTableEdge) inMemoryTableElement, edgeMutation.getAlterEdgeLabelTimestamp(), edgeMutation.getNewEdgeLabel());
            }
        }

        if (mutation.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            updateElementAndExtendedDataInSearchIndex(element, mutation, user);
        }
    }

    public Vertex saveVertexBuilder(VertexBuilder vertexBuilder, long timestamp, User user) {
        graph.addValidAuthorizations(user.getAuthorizations());
        boolean isNew = false;
        InMemoryTableElement vertexTableElement = vertices.getTableElement(vertexBuilder.getId());
        if (vertexTableElement == null) {
            isNew = true;
            vertices.append(
                vertexBuilder.getId(),
                new AlterVisibilityMutation(timestamp, vertexBuilder.getVisibility(), null),
                new ElementTimestampMutation(timestamp)
            );
        } else {
            if (vertexTableElement.getVisibility().equals(vertexBuilder.getVisibility())) {
                vertices.append(vertexBuilder.getId(), new ElementTimestampMutation(timestamp));
            } else {
                vertices.append(
                    vertexBuilder.getId(),
                    new AlterVisibilityMutation(timestamp, vertexBuilder.getVisibility(), null), new ElementTimestampMutation(timestamp)
                );
            }
        }
        InMemoryVertex vertex = vertices.get(graph, vertexBuilder.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        if (isNew) {
            fireGraphEvent(new AddVertexEvent(graph, vertex));
        }

        update(vertexBuilder, vertex, user);

        // to more closely simulate how accumulo works. add a potentially sparse (in case of an update) vertex to the search index.
        if (vertexBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            updateElementAndExtendedDataInSearchIndex(vertex, vertexBuilder, user);
        }

        return vertex;
    }

    public Edge savePreparedEdge(EdgeBuilderBase edgeBuilder, String outVertexId, String inVertexId, long timestamp, User user) {
        long incrementingTimestamp = timestamp;
        InMemoryTableElement edgeTableElement = this.edges.getTableElement(edgeBuilder.getId());
        boolean isNew = false;
        if (edgeTableElement == null) {
            isNew = true;
            edges.append(
                edgeBuilder.getId(),
                new AlterVisibilityMutation(incrementingTimestamp++, edgeBuilder.getVisibility(), null),
                new ElementTimestampMutation(incrementingTimestamp++),
                new AlterEdgeLabelMutation(incrementingTimestamp++, edgeBuilder.getEdgeLabel()),
                new EdgeSetupMutation(incrementingTimestamp++, outVertexId, inVertexId)
            );
        } else {
            edges.append(edgeBuilder.getId(), new ElementTimestampMutation(incrementingTimestamp++));
            if (edgeBuilder.getNewEdgeLabel() == null) {
                AlterEdgeLabelMutation alterEdgeLabelMutation = (AlterEdgeLabelMutation) edgeTableElement.findLastMutation(AlterEdgeLabelMutation.class);
                if (alterEdgeLabelMutation != null && !alterEdgeLabelMutation.getNewEdgeLabel().equals(edgeBuilder.getEdgeLabel())) {
                    edges.append(edgeBuilder.getId(), new AlterEdgeLabelMutation(incrementingTimestamp++, edgeBuilder.getEdgeLabel()));
                }
            }
        }
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edges.append(edgeBuilder.getId(), new AlterEdgeLabelMutation(incrementingTimestamp, edgeBuilder.getNewEdgeLabel()));
        }

        InMemoryEdge edge = this.edges.get(graph, edgeBuilder.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        if (isNew) {
            fireGraphEvent(new AddEdgeEvent(graph, edge));
        }
        update(edgeBuilder, edge, user);

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            updateElementAndExtendedDataInSearchIndex(edge, edgeBuilder, user);
        }

        return edge;
    }

    public <TElement extends Element, TInMemoryElement extends InMemoryElement> void update(
        ElementMutation<TElement> m,
        InMemoryElement<TInMemoryElement> element,
        User user
    ) {
        InMemoryTableElement<TInMemoryElement> inMemoryTableElement = element.getInMemoryTableElement();

        for (Property property : m.getProperties()) {
            addPropertyValue(
                element,
                inMemoryTableElement,
                property.getKey(),
                property.getName(),
                property.getValue(),
                property.getMetadata(),
                property.getVisibility(),
                property.getTimestamp(),
                user
            );
        }

        for (PropertyDeleteMutation propertyDeleteMutation : m.getPropertyDeletes()) {
            deleteProperty(
                element,
                inMemoryTableElement,
                propertyDeleteMutation.getKey(),
                propertyDeleteMutation.getName(),
                propertyDeleteMutation.getVisibility(),
                user
            );
        }

        for (PropertySoftDeleteMutation propertySoftDeleteMutation : m.getPropertySoftDeletes()) {
            Property property = element.getProperty(
                propertySoftDeleteMutation.getKey(),
                propertySoftDeleteMutation.getName(),
                propertySoftDeleteMutation.getVisibility()
            );
            if (property != null) {
                softDeleteProperty(
                    inMemoryTableElement,
                    property,
                    propertySoftDeleteMutation.getTimestamp(),
                    propertySoftDeleteMutation.getData(),
                    m.getIndexHint(),
                    user
                );
            }
        }

        for (AdditionalVisibilityAddMutation additionalVisibility : m.getAdditionalVisibilities()) {
            addAdditionalVisibility(
                inMemoryTableElement,
                additionalVisibility.getAdditionalVisibility(),
                additionalVisibility.getEventData(),
                user
            );
        }

        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : m.getAdditionalVisibilityDeletes()) {
            deleteAdditionalVisibility(
                inMemoryTableElement,
                additionalVisibilityDelete.getAdditionalVisibility(),
                additionalVisibilityDelete.getEventData(),
                user
            );
        }

        for (ElementMutationBase.DeleteExtendedDataRowData data : m.getDeleteExtendedDataRowData()) {
            deleteExtendedDataRow(
                element,
                data.getTableName(),
                data.getRow(),
                user
            );
        }

        for (ExtendedDataMutation extendedData : m.getExtendedData()) {
            graph.ensurePropertyDefined(extendedData.getColumnName(), extendedData.getValue());
            ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
                ElementType.getTypeFromElement(element),
                element.getId(),
                extendedData.getTableName(),
                extendedData.getRow()
            );
            extendedData(element, extendedDataRowId, extendedData, user);
        }

        for (ExtendedDataDeleteMutation extendedDataDelete : m.getExtendedDataDeletes()) {
            deleteExtendedData(
                element,
                extendedDataDelete.getTableName(),
                extendedDataDelete.getRow(),
                extendedDataDelete.getColumnName(),
                extendedDataDelete.getKey(),
                extendedDataDelete.getVisibility(),
                user
            );
        }

        for (AdditionalExtendedDataVisibilityAddMutation additionalVisibility : m.getAdditionalExtendedDataVisibilities()) {
            addAdditionalExtendedDataVisibility(
                element,
                additionalVisibility.getTableName(),
                additionalVisibility.getRow(),
                additionalVisibility.getAdditionalVisibility()
            );
        }

        for (AdditionalExtendedDataVisibilityDeleteMutation additionalVisibilityDelete : m.getAdditionalExtendedDataVisibilityDeletes()) {
            deleteAdditionalExtendedDataVisibility(
                element,
                additionalVisibilityDelete.getTableName(),
                additionalVisibilityDelete.getRow(),
                additionalVisibilityDelete.getAdditionalVisibility()
            );
        }

        for (ElementMutationBase.MarkPropertyVisibleData data : m.getMarkPropertyVisibleData()) {
            markPropertyVisible(
                element,
                inMemoryTableElement,
                data.getKey(),
                data.getName(),
                data.getPropertyVisibility(),
                data.getTimestamp(),
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkPropertyHiddenData data : m.getMarkPropertyHiddenData()) {
            markPropertyHidden(
                element,
                inMemoryTableElement,
                data.getKey(),
                data.getName(),
                data.getPropertyVisibility(),
                data.getTimestamp(),
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkVisibleData data : m.getMarkVisibleData()) {
            markElementVisible(
                element,
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        for (ElementMutationBase.MarkHiddenData data : m.getMarkHiddenData()) {
            markElementHidden(
                element,
                data.getVisibility(),
                data.getEventData(),
                user
            );
        }

        ElementMutationBase.SoftDeleteData softDeleteData = m.getSoftDeleteData();
        if (softDeleteData != null) {
            softDeleteElement(element, softDeleteData.getTimestamp(), softDeleteData.getEventData(), user);
        }

        if (m.isDeleteElement()) {
            deleteElement(element, user);
        }
    }

    private void addPropertyValue(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Object value,
        Metadata metadata,
        Visibility visibility,
        Long timestamp,
        User user
    ) {
        graph.ensurePropertyDefined(name, value);

        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        if (value instanceof StreamingPropertyValue) {
            value = saveStreamingPropertyValue((StreamingPropertyValue) value);
        }
        inMemoryTableElement.appendAddPropertyValueMutation(key, name, value, metadata, visibility, timestamp, null);
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, user);

        fireGraphEvent(new AddPropertyEvent(graph, element, property));
    }

    private void deleteProperty(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Visibility visibility,
        User user
    ) {
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, user);
        inMemoryTableElement.deleteProperty(key, name, visibility, user);

        graph.getSearchIndex().deleteProperty(graph, element, PropertyDescriptor.fromProperty(property), user);

        fireGraphEvent(new DeletePropertyEvent(graph, element, property));
    }

    private void softDeleteProperty(
        InMemoryTableElement inMemoryTableElement,
        Property property,
        Long timestamp,
        Object data,
        IndexHint indexHint,
        User user
    ) {
        Element element;
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp, data);
            element = graph.getVertex(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp, data);
            element = graph.getEdge(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        if (indexHint != IndexHint.DO_NOT_INDEX) {
            graph.getSearchIndex().deleteProperty(graph, element, PropertyDescriptor.fromProperty(property), user);
        }

        fireGraphEvent(new SoftDeletePropertyEvent(graph, element, property, data));
    }

    private void addAdditionalVisibility(
        InMemoryTableElement inMemoryTableElement,
        String visibility,
        Object eventData,
        User user
    ) {
        Element element;
        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.ALL_INCLUDING_HIDDEN)
            .setIgnoreAdditionalVisibilities(true)
            .build();
        inMemoryTableElement.appendAddAdditionalVisibilityMutation(visibility, eventData);
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            element = graph.getVertex(inMemoryTableElement.getId(), fetchHints, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            element = graph.getEdge(inMemoryTableElement.getId(), fetchHints, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        fireGraphEvent(new AddAdditionalVisibilityEvent(graph, element, visibility, eventData));
    }

    private void deleteAdditionalVisibility(
        InMemoryTableElement inMemoryTableElement,
        String visibility,
        Object eventData,
        User user
    ) {
        Element element;
        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.ALL_INCLUDING_HIDDEN)
            .setIgnoreAdditionalVisibilities(true)
            .build();
        inMemoryTableElement.appendDeleteAdditionalVisibilityMutation(visibility, eventData);
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            element = graph.getVertex(inMemoryTableElement.getId(), fetchHints, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            element = graph.getEdge(inMemoryTableElement.getId(), fetchHints, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        fireGraphEvent(new DeleteAdditionalVisibilityEvent(graph, element, visibility, eventData));
    }

    private <T extends InMemoryElement> void deleteExtendedDataRow(
        InMemoryElement<T> element,
        String tableName,
        String rowId,
        User user
    ) {
        ExtendedDataRowId id = new ExtendedDataRowId(
            ElementType.getTypeFromElement(element),
            element.getId(),
            tableName,
            rowId
        );
        List<ExtendedDataRow> rows = graph.getExtendedData(Lists.newArrayList(id), user).collect(Collectors.toList());
        if (rows.size() > 1) {
            throw new VertexiumException("Found too many extended data rows for id: " + id);
        }
        if (rows.size() != 1) {
            return;
        }

        extendedDataTable.remove(id);
        graph.getSearchIndex().deleteExtendedData(graph, id, user);

        fireGraphEvent(new DeleteExtendedDataRowEvent(graph, id));
    }

    private void deleteExtendedData(
        InMemoryElement element,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    ) {
        extendedDataTable.removeColumn(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            columnName,
            key,
            visibility
        );

        graph.getSearchIndex().deleteExtendedData(graph, element, tableName, row, columnName, key, visibility, user);
        fireGraphEvent(new DeleteExtendedDataEvent(graph, element, tableName, row, columnName, key));
    }

    private void addAdditionalExtendedDataVisibility(
        InMemoryElement element,
        String tableName,
        String row,
        String additionalVisibility
    ) {
        extendedDataTable.addAdditionalVisibility(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            additionalVisibility
        );

        fireGraphEvent(new AddAdditionalExtendedDataVisibilityEvent(graph, element, tableName, row, additionalVisibility));
    }

    private void deleteAdditionalExtendedDataVisibility(
        InMemoryElement element,
        String tableName,
        String row,
        String additionalVisibility
    ) {
        extendedDataTable.deleteAdditionalVisibility(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            additionalVisibility
        );

        fireGraphEvent(new DeleteAdditionalExtendedDataVisibilityEvent(graph, element, tableName, row, additionalVisibility));
    }

    private void extendedData(
        Element element,
        ExtendedDataRowId rowId,
        ExtendedDataMutation extendedData,
        User user
    ) {
        extendedDataTable.addData(rowId, extendedData.getColumnName(), extendedData.getKey(), extendedData.getValue(), extendedData.getTimestamp(), extendedData.getVisibility());
        graph.getSearchIndex().addElementExtendedData(
            graph,
            element,
            Collections.singleton(extendedData),
            Collections.emptyList(),
            Collections.emptyList(),
            user
        );
        fireGraphEvent(new AddExtendedDataEvent(
            graph,
            element,
            rowId.getTableName(),
            rowId.getRowId(),
            extendedData.getColumnName(),
            extendedData.getKey(),
            extendedData.getValue(),
            extendedData.getVisibility()
        ));
    }

    private void markPropertyVisible(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object data,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        Property property = inMemoryTableElement.appendMarkPropertyVisibleMutation(
            key,
            name,
            propertyVisibility,
            timestamp,
            visibility,
            data,
            user
        );

        graph.getSearchIndex().markPropertyVisible(graph, element, property, visibility, user);

        if (property != null) {
            fireGraphEvent(new MarkVisiblePropertyEvent(graph, element, property, visibility, data));
        }
    }

    private void markPropertyHidden(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object data,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        Property hiddenProperty = inMemoryTableElement.appendMarkPropertyHiddenMutation(
            propertyKey,
            propertyName,
            propertyVisibility,
            timestamp,
            visibility,
            data,
            user
        );

        graph.getSearchIndex().markPropertyHidden(graph, element, hiddenProperty, visibility, user);

        if (hiddenProperty != null) {
            fireGraphEvent(new MarkHiddenPropertyEvent(graph, element, hiddenProperty, visibility, data));
        }
    }

    private <T extends InMemoryElement> void markElementHidden(
        InMemoryElement<T> element,
        Visibility visibility,
        Object eventData,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        if (element instanceof Edge) {
            Edge edge = (Edge) element;

            Vertex inVertex = graph.getVertex(edge.getVertexId(Direction.IN), FetchHints.ALL_INCLUDING_HIDDEN, user);
            checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
            Vertex outVertex = graph.getVertex(edge.getVertexId(Direction.OUT), FetchHints.ALL_INCLUDING_HIDDEN, user);
            checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

            edges.getTableElement(edge.getId()).appendMarkHiddenMutation(visibility, eventData);
            graph.getSearchIndex().markElementHidden(graph, edge, visibility, user);

            fireGraphEvent(new MarkHiddenEdgeEvent(graph, edge, eventData));
        } else if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;

            vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, user)
                .forEach(edgeToMarkHidden -> markElementHidden((InMemoryEdge) edgeToMarkHidden, visibility, eventData, user));

            vertices.getTableElement(vertex.getId()).appendMarkHiddenMutation(visibility, eventData);
            refreshVertexInMemoryTableElement(vertex);
            graph.getSearchIndex().markElementHidden(graph, vertex, visibility, user);

            fireGraphEvent(new MarkHiddenVertexEvent(graph, vertex, eventData));
        } else {
            throw new VertexiumException("Unhandled element type: " + element);
        }
    }

    private <T extends InMemoryElement> void markElementVisible(
        InMemoryElement<T> element,
        Visibility visibility,
        Object eventData,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        if (element instanceof Edge) {
            Edge edge = (Edge) element;

            Vertex inVertex = graph.getVertex(edge.getVertexId(Direction.IN), FetchHints.ALL_INCLUDING_HIDDEN, user);
            checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
            Vertex outVertex = graph.getVertex(edge.getVertexId(Direction.OUT), FetchHints.ALL_INCLUDING_HIDDEN, user);
            checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

            edges.getTableElement(edge.getId()).appendMarkVisibleMutation(visibility, eventData);
            graph.getSearchIndex().markElementVisible(graph, edge, visibility, user);

            fireGraphEvent(new MarkVisibleEdgeEvent(graph, edge, eventData));
        } else if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;

            vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, user)
                .forEach(edgeToMarkVisible -> markElementVisible((InMemoryEdge) edgeToMarkVisible, visibility, eventData, user));

            vertices.getTableElement(vertex.getId()).appendMarkVisibleMutation(visibility, eventData);
            refreshVertexInMemoryTableElement(vertex);
            graph.getSearchIndex().markElementVisible(graph, vertex, visibility, user);

            fireGraphEvent(new MarkVisibleVertexEvent(graph, vertex, eventData));
        } else {
            throw new VertexiumException("Unhandled element type: " + element);
        }
    }

    private <T extends InMemoryElement> void softDeleteElement(
        InMemoryElement<T> element,
        Long timestamp,
        Object eventData,
        User user
    ) {
        checkNotNull(element, "Element cannot be null");
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        long timestampFinal = timestamp;

        if (!element.canRead(user)) {
            return;
        }

        for (Property property : element.getProperties()) {
            softDeleteProperty(
                element.getInMemoryTableElement(),
                property,
                timestamp,
                eventData,
                IndexHint.INDEX,
                user
            );
        }

        if (element instanceof Edge) {
            Edge edge = (Edge) element;

            edges.getTableElement(edge.getId()).appendSoftDeleteMutation(timestamp, eventData);

            graph.getSearchIndex().deleteElement(graph, edge, user);

            fireGraphEvent(new SoftDeleteEdgeEvent(graph, edge, eventData));
        } else if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            if (!((InMemoryVertex) vertex).canRead(user)) {
                return;
            }

            vertex.getEdges(Direction.BOTH, user)
                .forEach(edgeToSoftDelete -> softDeleteElement((InMemoryEdge) edgeToSoftDelete, timestampFinal, eventData, user));

            vertices.getTableElement(vertex.getId()).appendSoftDeleteMutation(timestamp, eventData);

            graph.getSearchIndex().deleteElement(graph, vertex, user);

            fireGraphEvent(new SoftDeleteVertexEvent(graph, vertex, eventData));
        } else {
            throw new VertexiumException("Unhandled element type: " + element);
        }
    }

    private void deleteElement(InMemoryElement element, User user) {
        if (element instanceof Edge) {
            Edge edge = (Edge) element;

            checkNotNull(edge, "Edge cannot be null");
            if (!((InMemoryEdge) edge).canRead(user)) {
                return;
            }

            deleteAllExtendedDataForElement(edge, user);

            edges.remove(edge.getId());
            graph.getSearchIndex().deleteElement(graph, edge, user);

            fireGraphEvent(new DeleteEdgeEvent(graph, edge));
        } else if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            if (!((InMemoryVertex) vertex).canRead(user)) {
                return;
            }

            vertex.getEdges(Direction.BOTH, user)
                .forEach(edgeToDelete -> deleteElement((InMemoryElement) edgeToDelete, user));

            deleteAllExtendedDataForElement(vertex, user);

            vertices.remove(vertex.getId());
            graph.getSearchIndex().deleteElement(graph, vertex, user);

            fireGraphEvent(new DeleteVertexEvent(graph, vertex));
        } else {
            throw new VertexiumException("Unhandled element type: " + element);
        }
    }

    private void refreshVertexInMemoryTableElement(Vertex vertex) {
        ((InMemoryVertex) vertex).setInMemoryTableElement(vertices.getTableElement(vertex.getId()));
    }

    private StreamingPropertyValueRef saveStreamingPropertyValue(StreamingPropertyValue value) {
        return new InMemoryStreamingPropertyValueRef(value);
    }

    private void alterElementVisibility(InMemoryTableElement inMemoryTableElement, Visibility newEdgeVisibility, Object data) {
        inMemoryTableElement.appendAlterVisibilityMutation(newEdgeVisibility, data);
    }

    private void alterElementPropertyVisibilities(
        InMemoryTableElement inMemoryTableElement,
        List<AlterPropertyVisibility> alterPropertyVisibilities,
        User user
    ) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = inMemoryTableElement.getProperty(
                apv.getKey(),
                apv.getName(),
                apv.getExistingVisibility(),
                FetchHints.ALL_INCLUDING_HIDDEN,
                user
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            Object value = property.getValue();
            Metadata metadata = property.getMetadata();

            inMemoryTableElement.appendSoftDeletePropertyMutation(
                apv.getKey(),
                apv.getName(),
                apv.getExistingVisibility(),
                apv.getTimestamp(),
                apv.getData()
            );

            long newTimestamp = apv.getTimestamp() + 1;
            if (value instanceof StreamingPropertyValue) {
                value = saveStreamingPropertyValue((StreamingPropertyValue) value);
            }
            inMemoryTableElement.appendAddPropertyValueMutation(
                apv.getKey(),
                apv.getName(),
                value,
                metadata,
                apv.getVisibility(),
                newTimestamp,
                apv.getData()
            );
        }
    }

    private void alterElementPropertyMetadata(
        InMemoryTableElement inMemoryTableElement,
        List<SetPropertyMetadata> setPropertyMetadatas,
        User user
    ) {
        for (SetPropertyMetadata spm : setPropertyMetadatas) {
            Property property = inMemoryTableElement.getProperty(
                spm.getPropertyKey(),
                spm.getPropertyName(),
                spm.getPropertyVisibility(),
                FetchHints.ALL_INCLUDING_HIDDEN,
                user
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + spm.getPropertyKey() + ":" + spm.getPropertyName());
            }

            Metadata metadata = Metadata.create(property.getMetadata());
            metadata.add(spm.getMetadataName(), spm.getNewValue(), spm.getMetadataVisibility());

            long newTimestamp = IncreasingTime.currentTimeMillis();
            inMemoryTableElement.appendAddPropertyMetadataMutation(
                property.getKey(), property.getName(), metadata, property.getVisibility(), newTimestamp);
        }
    }

    private void alterEdgeLabel(InMemoryTableEdge inMemoryTableEdge, long timestamp, String newEdgeLabel) {
        inMemoryTableEdge.appendAlterEdgeLabelMutation(timestamp, newEdgeLabel);
    }

    private void deleteAllExtendedDataForElement(Element element, User user) {
        if (!element.getFetchHints().isIncludeExtendedDataTableNames() || element.getExtendedDataTableNames().size() <= 0) {
            return;
        }

        ExistingElementMutation<Element> m = element.prepareMutation();
        for (ExtendedDataRow row : graph.getExtendedData(ElementType.getTypeFromElement(element), element.getId(), null, user)) {
            m.deleteExtendedDataRow(row.getId());
        }
        m.save(user);
    }

    private <T extends Element> void updateElementAndExtendedDataInSearchIndex(
        Element element,
        ElementMutation<T> elementMutation,
        User user
    ) {
        if (elementMutation instanceof ExistingElementMutation) {
            graph.getSearchIndex().updateElement(graph, (ExistingElementMutation<? extends Element>) elementMutation, user);
        } else {
            graph.getSearchIndex().addElement(graph, element, user);
        }
        graph.getSearchIndex().addElementExtendedData(
            graph,
            element,
            elementMutation.getExtendedData(),
            elementMutation.getAdditionalExtendedDataVisibilities(),
            elementMutation.getAdditionalExtendedDataVisibilityDeletes(),
            user
        );
        for (ExtendedDataDeleteMutation m : elementMutation.getExtendedDataDeletes()) {
            graph.getSearchIndex().deleteExtendedData(
                graph,
                element,
                m.getTableName(),
                m.getRow(),
                m.getColumnName(),
                m.getKey(),
                m.getVisibility(),
                user
            );
        }
    }
}
