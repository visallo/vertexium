package org.vertexium.accumulo;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.event.*;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.ArrayUtils;
import org.vertexium.util.ExtendedDataMutationUtils;
import org.vertexium.util.IncreasingTime;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.vertexium.mutation.ElementMutationBase.*;
import static org.vertexium.util.IncreasingTime.currentTimeMillis;

public abstract class ElementMutationBuilder {
    public static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value("".getBytes());

    private final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    private final VertexiumSerializer vertexiumSerializer;
    private static final Cache<String, Text> propertyMetadataColumnQualifierTextCache = CacheBuilder
        .newCache(String.class, Text.class)
        .name(ElementMutationBuilder.class, "propertyMetadataColumnQualifierTextCache")
        .maxSize(10000)
        .build();

    protected ElementMutationBuilder(
        StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy,
        VertexiumSerializer vertexiumSerializer
    ) {
        this.streamingPropertyValueStorageStrategy = streamingPropertyValueStorageStrategy;
        this.vertexiumSerializer = vertexiumSerializer;
    }

    public void saveVertexMutation(AccumuloGraph graph, ElementMutation<Vertex> vertexBuilder, long timestamp, User user) {
        String vertexRowKey = vertexBuilder.getId();
        Mutation vertexMutation = new Mutation(vertexRowKey);

        if (!vertexBuilder.isDeleteElement()) {
            Visibility visibility = vertexBuilder.getVisibility();
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility == null ?
                    getVertexFromMutation(graph, vertexBuilder, user).getVisibility() : visibility);
            vertexMutation.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, columnVisibility, timestamp, EMPTY_VALUE);
        }
        addElementMutationsToAccumuloMutation(graph, vertexBuilder, vertexRowKey, vertexMutation);
        saveVertexMutations(vertexMutation);
        saveExtendedDataMutations(graph, vertexBuilder, user);
        graph.flush();

        // We only need the vertex in certain situations, loading it like this lets it be lazy
        Supplier<Vertex> vertex = Suppliers.memoize(() -> getVertexFromMutation(graph, vertexBuilder, user));
        if (vertexBuilder.isDeleteElement()) {
            graph.getSearchIndex().deleteElement(graph, vertex.get(), user);

            vertex.get().getEdges(Direction.BOTH, user).forEach(edge -> {
                EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().deleteElement();
                saveEdgeMutation(graph, edgeMutation, timestamp, user);
            });
        } else if (vertexBuilder.getSoftDeleteData() != null) {
            graph.getSearchIndex().deleteElement(graph, vertex.get(), user);

            SoftDeleteData data = vertexBuilder.getSoftDeleteData();
            vertex.get().getEdges(Direction.BOTH, user).forEach(edge -> {
                EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().softDeleteElement(data.getTimestamp(), data.getEventData());
                saveEdgeMutation(graph, edgeMutation, timestamp, user);
            });
        } else {
            if (vertexBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
                // TODO: allow for addOrUpdate by mutation
                graph.getSearchIndex().addElement(graph, vertex.get(), user);
                // TODO: timing issue? If this is an add with hidden/visible the ES requests may not finish in proper order?
            }

            vertexBuilder.getMarkVisibleData().forEach(data -> {
                graph.getSearchIndex().markElementVisible(graph, vertex.get(), data.getVisibility(), user);

                vertex.get().getEdges(Direction.BOTH, user).forEach(edge -> {
                    EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().markElementVisible(data.getVisibility(), data.getEventData()).setIndexHint(vertexBuilder.getIndexHint());
                    saveEdgeMutation(graph, edgeMutation, timestamp, user);
                });
            });
            vertexBuilder.getMarkHiddenData().forEach(data -> {
                graph.getSearchIndex().markElementHidden(graph, vertex.get(), data.getVisibility(), user);

                vertex.get().getEdges(Direction.BOTH, user).forEach(edge -> {
                    EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().markElementHidden(data.getVisibility(), data.getEventData()).setIndexHint(vertexBuilder.getIndexHint());
                    saveEdgeMutation(graph, edgeMutation, timestamp, user);
                });
            });
        }

        // TODO: In all of the above cases where we delete/modify edges, it would be more efficient to collect all of the vertex mutations and submit them as a batch

        queueEvents(graph, vertex, vertexBuilder);
    }

    private Vertex getVertexFromMutation(AccumuloGraph graph, ElementMutation<Vertex> vertexBuilder, User user) {
        Vertex vertex = vertexBuilder instanceof ExistingElementMutation ?
                ((ExistingElementMutation<Vertex>) vertexBuilder).getElement() :
                graph.getVertex(vertexBuilder.getId(), FetchHints.EDGE_REFS, user);
        if (vertex == null) {
            throw new VertexiumException("Expected to find vertex but was unable to load: " + vertexBuilder.getId());
        }
        return  vertex;

    }

    private <T extends Element> void saveExtendedDataMutations(AccumuloGraph graph, ElementMutation<T> elementBuilder, User user) {
        saveExtendedData(
            graph,
            elementBuilder,
            elementBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX,
            elementBuilder.getExtendedData(),
            elementBuilder.getExtendedDataDeletes(),
            elementBuilder.getAdditionalExtendedDataVisibilities(),
            elementBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            user
        );
    }

    void saveExtendedData(
        AccumuloGraph graph,
        ElementLocation elementLocation,
        boolean updateIndex,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    ) {
        Map<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowId = ExtendedDataMutationUtils.getByTableThenRowId(
            extendedData,
            extendedDataDeletes,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes
        );

        for (Map.Entry<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowIdEntry : byTableThenRowId.entrySet()) {
            String tableName = byTableThenRowIdEntry.getKey();
            Map<String, ExtendedDataMutationUtils.Mutations> byRowId = byTableThenRowIdEntry.getValue();
            for (Map.Entry<String, ExtendedDataMutationUtils.Mutations> byRowIdEntry : byRowId.entrySet()) {
                String row = byRowIdEntry.getKey();
                ExtendedDataMutationUtils.Mutations mutations = byRowIdEntry.getValue();

                Mutation m = new Mutation(KeyHelper.createExtendedDataRowKey(elementLocation.getElementType(), elementLocation.getId(), tableName, row));

                for (ExtendedDataMutation edm : mutations.getExtendedData()) {
                    Object value = transformValue(edm.getValue(), null, null);

                    // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
                    if (graph != null) {
                        graph.ensurePropertyDefined(edm.getColumnName(), value);
                    }

                    m.put(
                        AccumuloElement.CF_EXTENDED_DATA,
                        KeyHelper.createExtendedDataColumnQualifier(edm),
                        visibilityToAccumuloVisibility(edm.getVisibility()),
                        new Value(vertexiumSerializer.objectToBytes(value))
                    );
                }

                for (ExtendedDataDeleteMutation eddm : mutations.getExtendedDataDeletes()) {
                    m.putDelete(
                        AccumuloElement.CF_EXTENDED_DATA,
                        KeyHelper.createExtendedDataColumnQualifier(eddm),
                        visibilityToAccumuloVisibility(eddm.getVisibility())
                    );
                }

                for (AdditionalExtendedDataVisibilityAddMutation add : mutations.getAdditionalExtendedDataVisibilities()) {
                    Value value = toAddAdditionalVisibilityValue(add.getEventData());
                    m.put(
                        AccumuloElement.CF_ADDITIONAL_VISIBILITY,
                        new Text(add.getAdditionalVisibility()),
                        new ColumnVisibility(),
                        value
                    );
                }

                for (AdditionalExtendedDataVisibilityDeleteMutation del : mutations.getAdditionalExtendedDataVisibilityDeletes()) {
                    Value value = toDeleteAdditionalVisibilityValue(del.getEventData());
                    m.put(
                        AccumuloElement.CF_ADDITIONAL_VISIBILITY,
                        new Text(del.getAdditionalVisibility()),
                        new ColumnVisibility(),
                        value
                    );
                }

                saveExtendedDataMutation(elementLocation.getElementType(), m);
            }
        }

        if (updateIndex) {
            graph.getSearchIndex().addElementExtendedData(
                    graph,
                    elementLocation,
                    extendedData,
                    additionalExtendedDataVisibilities,
                    additionalExtendedDataVisibilityDeletes,
                    user
            );
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                graph.getSearchIndex().deleteExtendedData(
                        graph,
                        elementLocation,
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

    protected abstract void saveExtendedDataMutation(ElementType elementType, Mutation m);

    protected abstract void saveVertexMutations(Mutation... m);

    private <T extends Element> void addElementMutationsToAccumuloMutation(AccumuloGraph graph, ElementMutation<T> elementMutation, String rowKey, Mutation m) {
        if (elementMutation.isDeleteElement()) {
            m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
            return;
        }

        // TODO: handle alter property metadata

        // TODO: handle alter property visibility

        for (PropertyDeleteMutation propertyDeleteMutation : elementMutation.getPropertyDeletes()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : elementMutation.getPropertySoftDeletes()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
        }
        for (Property property : elementMutation.getProperties()) {
            addPropertyToMutation(graph, m, rowKey, property);
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : elementMutation.getAdditionalVisibilities()) {
            addAdditionalVisibilityToMutation(m, additionalVisibility);
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : elementMutation.getAdditionalVisibilityDeletes()) {
            addAdditionalVisibilityDeleteToMutation(m, additionalVisibilityDelete);
        }

        // TODO: handle cleaning up the extended data markers for extended data deletions. need to elevate to do this properly

        Iterable<ExtendedDataMutation> extendedData = elementMutation.getExtendedData();
        saveExtendedDataMarkers(m, extendedData);

        for (MarkPropertyVisibleData markPropertyVisibleData : elementMutation.getMarkPropertyVisibleData()) {
            addMarkPropertyVisibleToMutation(m, markPropertyVisibleData);
        }
        for (MarkPropertyHiddenData markPropertyHiddenData : elementMutation.getMarkPropertyHiddenData()) {
            addMarkPropertyHiddenToMutation(m, markPropertyHiddenData);
        }
        for (MarkVisibleData markVisibleData : elementMutation.getMarkVisibleData()) {
            addMarkVisibleToMutation(m, markVisibleData);
        }
        for (MarkHiddenData markHiddenData : elementMutation.getMarkHiddenData()) {
            addMarkHiddenToMutation(m, markHiddenData);
        }
        if (elementMutation.getSoftDeleteData() != null) {
            addSoftDeleteToMutation(m, elementMutation.getSoftDeleteData());
        }
    }

    public void saveExtendedDataMarkers(
        String elementId,
        ElementType elementType,
        Iterable<ExtendedDataMutation> extendedData
    ) {
        Set<TableNameVisibilityPair> uniquePairs = TableNameVisibilityPair.getUniquePairs(extendedData);
        if (uniquePairs.size() == 0) {
            return;
        }
        Mutation m = new Mutation(elementId);
        for (TableNameVisibilityPair pair : uniquePairs) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
        saveElementMutation(elementType, m);
    }

    private void saveElementMutation(ElementType elementType, Mutation m) {
        switch (elementType) {
            case VERTEX:
                saveVertexMutations(m);
                break;
            case EDGE:
                saveEdgeMutations(m);
                break;
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    private void saveExtendedDataMarkers(Mutation m, Iterable<ExtendedDataMutation> extendedData) {
        for (TableNameVisibilityPair pair : TableNameVisibilityPair.getUniquePairs(extendedData)) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
    }

    private void addExtendedDataMarkerToElementMutation(Mutation m, TableNameVisibilityPair pair) {
        m.put(
            AccumuloElement.CF_EXTENDED_DATA,
            new Text(pair.getTableName()),
            visibilityToAccumuloVisibility(pair.getVisibility()),
            new Value(pair.getTableName().getBytes())
        );
    }

    public Iterable<KeyValuePair> getKeyValuePairsForVertex(AccumuloVertex vertex) {
        List<KeyValuePair> results = new ArrayList<>();
        Text vertexRowKey = new Text(vertex.getId());
        results.add(new KeyValuePair(new Key(vertexRowKey, AccumuloVertex.CF_SIGNAL, ElementMutationBuilder.EMPTY_TEXT, visibilityToAccumuloVisibility(vertex.getVisibility()), vertex.getTimestamp()), EMPTY_VALUE));
        if (vertex.getPropertyDeleteMutations().iterator().hasNext()) {
            throw new VertexiumException("Cannot get key/value pairs for property deletions");
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : vertex.getPropertySoftDeleteMutations()) {
            addPropertySoftDeleteToKeyValuePairs(results, vertexRowKey, propertySoftDeleteMutation);
        }
        for (Property property : vertex.getProperties()) {
            addPropertyToKeyValuePairs(results, vertexRowKey, property);
        }
        return results;
    }

    public Iterable<KeyValuePair> getEdgeTableKeyValuePairsEdge(AccumuloEdge edge) {
        List<KeyValuePair> results = new ArrayList<>();

        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        Text edgeRowKey = new Text(edge.getId());
        String edgeLabel = edge.getLabel();
        if (edge.getNewEdgeLabel() != null) {
            throw new VertexiumException("Cannot get key/value pairs for label changes");
        }
        results.add(new KeyValuePair(new Key(edgeRowKey, AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, edge.getTimestamp()), ElementMutationBuilder.EMPTY_VALUE));
        results.add(new KeyValuePair(new Key(edgeRowKey, AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), edgeColumnVisibility, edge.getTimestamp()), ElementMutationBuilder.EMPTY_VALUE));
        results.add(new KeyValuePair(new Key(edgeRowKey, AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), edgeColumnVisibility, edge.getTimestamp()), ElementMutationBuilder.EMPTY_VALUE));
        if (edge.getPropertyDeleteMutations().iterator().hasNext()) {
            throw new VertexiumException("Cannot get key/value pairs for property deletions");
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : edge.getPropertySoftDeleteMutations()) {
            addPropertySoftDeleteToKeyValuePairs(results, edgeRowKey, propertySoftDeleteMutation);
        }
        for (Property property : edge.getProperties()) {
            addPropertyToKeyValuePairs(results, edgeRowKey, property);
        }
        return results;
    }

    public Iterable<KeyValuePair> getVertexTableKeyValuePairsEdge(AccumuloEdge edge) {
        List<KeyValuePair> results = new ArrayList<>();
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        String edgeLabel = edge.getNewEdgeLabel() != null ? edge.getNewEdgeLabel() : edge.getLabel();
        Text edgeIdText = new Text(edge.getId());
        long timestamp = edge.getTimestamp();
        Text visibility = new Text(edge.getVisibility().getVisibilityString());

        // out vertex.
        Text vertexOutIdRowKey = new Text(edge.getVertexId(Direction.OUT));
        org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.IN), visibility);
        results.add(new KeyValuePair(new Key(vertexOutIdRowKey, AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp), edgeInfo.toValue()));

        // in vertex.
        Text vertexInIdRowKey = new Text(edge.getVertexId(Direction.IN));
        edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.OUT), visibility);
        results.add(new KeyValuePair(new Key(vertexInIdRowKey, AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, timestamp), edgeInfo.toValue()));

        return results;
    }

    private void addPropertyToKeyValuePairs(List<KeyValuePair> results, Text elementRowKey, Property property) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = property.getValue();
        Value value = new Value(vertexiumSerializer.objectToBytes(transformValue(propertyValue, null, null)));
        results.add(new KeyValuePair(new Key(elementRowKey, AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, property.getTimestamp()), value));
        addPropertyMetadataToKeyValuePairs(results, elementRowKey, property);
    }

    private Object transformValue(Object propertyValue, String rowKey, Property property) {
        if (propertyValue instanceof StreamingPropertyValue) {
            if (rowKey != null && property != null) {
                propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
            } else {
                throw new VertexiumException(StreamingPropertyValue.class.getSimpleName() + " are not supported");
            }
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        return propertyValue;
    }

    private void addPropertyMetadataToKeyValuePairs(List<KeyValuePair> results, Text vertexRowKey, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            addPropertyMetadataItemToKeyValuePairs(results, vertexRowKey, property, metadataItem);
        }
    }

    private void addPropertyMetadataItemToKeyValuePairs(List<KeyValuePair> results, Text vertexRowKey, Property property, Metadata.Entry metadataItem) {
        Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem.getKey());
        ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
        if (metadataItem.getValue() == null) {
            throw new VertexiumException("Property metadata deletes are not supported");
        } else {
            addPropertyMetadataItemAddToKeyValuePairs(results, vertexRowKey, columnQualifier, metadataVisibility, property.getTimestamp(), metadataItem.getValue());
        }
    }

    private void addPropertyMetadataItemAddToKeyValuePairs(List<KeyValuePair> results, Text vertexRowKey, Text columnQualifier, ColumnVisibility metadataVisibility, long propertyTimestamp, Object value) {
        Value metadataValue = new Value(vertexiumSerializer.objectToBytes(value));
        results.add(new KeyValuePair(new Key(vertexRowKey, AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, propertyTimestamp), metadataValue));
    }

    private void addPropertySoftDeleteToKeyValuePairs(List<KeyValuePair> results, Text elementRowKey, PropertySoftDeleteMutation propertySoftDeleteMutation) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertySoftDeleteMutation.getKey(), propertySoftDeleteMutation.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertySoftDeleteMutation.getVisibility());
        results.add(
            new KeyValuePair(
                new Key(elementRowKey, AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDeleteMutation.getTimestamp()),
                toSoftDeleteDataToValue(propertySoftDeleteMutation.getData())
            )
        );
    }

    public void saveEdgeMutation(AccumuloGraph graph, EdgeMutation edgeBuilder, long timestamp, User user) {
        String edgeRowKey = edgeBuilder.getId();
        Mutation edgeMutation = new Mutation(edgeRowKey);
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edgeBuilder.getVisibility());

        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getEdgeLabel();
        if (!edgeBuilder.isDeleteElement()) {
            // TODO: handle element visibility changes

            if (edgeBuilder.getNewEdgeLabel() != null) {
                edgeMutation.putDelete(AccumuloEdge.CF_SIGNAL, new Text(getEdgeFromMutation(graph, edgeBuilder, user).getLabel()), edgeColumnVisibility, currentTimeMillis());
            }
            edgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
            edgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getVertexId(Direction.OUT)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
            edgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getVertexId(Direction.IN)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        }
        addElementMutationsToAccumuloMutation(graph, edgeBuilder, edgeRowKey, edgeMutation);
        saveEdgeMutations(edgeMutation);
        graph.flush();

        // We only need the edge in certain situations, loading it like this lets it be lazy
        Supplier<Edge> edge = Suppliers.memoize(() -> getEdgeFromMutation(graph, edgeBuilder, user));
        Mutation outMutation = new Mutation(edgeBuilder.getVertexId(Direction.OUT));
        Mutation inMutation = new Mutation(edgeBuilder.getVertexId(Direction.IN));
        Text edgeIdText = new Text(edgeBuilder.getId());
        if (edgeBuilder.isDeleteElement()) {
            graph.getSearchIndex().deleteElement(graph, edge.get(), user);

            outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility);
            inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility);
        } else if (edgeBuilder.getSoftDeleteData() != null) {
            graph.getSearchIndex().deleteElement(graph, edge.get(), user);

            Long softDeleteTimestamp = edgeBuilder.getSoftDeleteData().getTimestamp();
            if (softDeleteTimestamp == null) {
                softDeleteTimestamp = IncreasingTime.currentTimeMillis();
            }
            Value value = toSoftDeleteDataToValue(edgeBuilder.getSoftDeleteData().getEventData());
            outMutation.put(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE, edgeIdText, edgeColumnVisibility, softDeleteTimestamp, value);
            inMutation.put(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE, edgeIdText, edgeColumnVisibility, softDeleteTimestamp, value);
        } else {
            if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
                // TODO: allow for addOrUpdate by mutation
                graph.getSearchIndex().addElement(graph, edge.get(), user);
                // TODO: timing issue? If this is an add with hidden/visible the ES requests may not finish in proper order?
            }

            Text edgeInfoVisibility = new Text(edgeColumnVisibility.getExpression());

            // TODO: handle element visibility changes

            EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edgeBuilder.getVertexId(Direction.IN), edgeInfoVisibility);
            outMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());

            edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edgeBuilder.getVertexId(Direction.OUT));
            inMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());

            edgeBuilder.getMarkVisibleData().forEach(data -> {
                graph.getSearchIndex().markElementVisible(graph, edge.get(), data.getVisibility(), user);

                Value value = toHiddenDeletedValue(data.getEventData());
                ColumnVisibility hiddenVisibility = visibilityToAccumuloVisibility(data.getVisibility());
                outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, edgeIdText, hiddenVisibility, value);
                inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, edgeIdText, hiddenVisibility, value);
            });
            edgeBuilder.getMarkHiddenData().forEach(data -> {
                graph.getSearchIndex().markElementHidden(graph, edge.get(), data.getVisibility(), user);

                Value value = toHiddenValue(data.getEventData());
                ColumnVisibility hiddenVisibility = visibilityToAccumuloVisibility(data.getVisibility());
                outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, edgeIdText, hiddenVisibility, value);
                inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, edgeIdText, hiddenVisibility, value);
            });
        }
        saveVertexMutations(outMutation, inMutation);
        saveExtendedDataMutations(graph, edgeBuilder, user);

        queueEvents(graph, edge, edgeBuilder);
    }

    @SuppressWarnings("unchecked")
    private Edge getEdgeFromMutation(AccumuloGraph graph, EdgeMutation edgeBuilder, User user) {
        Edge edge = edgeBuilder instanceof ExistingElementMutation ?
                ((ExistingElementMutation<Edge>) edgeBuilder).getElement() :
                graph.getEdge(edgeBuilder.getId(), FetchHints.NONE, user);
        if (edge == null) {
            throw new VertexiumException("Expected to find edge but was unable to load: " + edgeBuilder.getId());
        }
        return  edge;
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutations(Mutation... m);

    public boolean alterElementVisibility(Mutation m, AccumuloElement element, Visibility newVisibility, Object data) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(element.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }

        if (element instanceof AccumuloEdge) {
            AccumuloEdge edge = (AccumuloEdge) element;
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility, currentTimeMillis(), toSignalDeletedValue(data));
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, currentTimeMillis(), toSignalValue(data));

            m.putDelete(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof AccumuloVertex) {
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility, currentTimeMillis(), toSignalDeletedValue(data));
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, currentTimeMillis(), toSignalValue(data));
        } else {
            throw new IllegalArgumentException("Invalid element type: " + element);
        }
        return true;
    }

    public boolean alterEdgeVertexOutVertex(Mutation vertexOutMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        Text newColumnVisibilityAsText = new Text(newColumnVisibility.getExpression());
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.IN), newColumnVisibilityAsText);
        vertexOutMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public boolean alterEdgeVertexInVertex(Mutation vertexInMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        Text newColumnVisibilityAsText = new Text(newColumnVisibility.getExpression());
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.OUT), newColumnVisibilityAsText);
        vertexInMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addAdditionalVisibilityToMutation(
        Mutation m,
        AdditionalVisibilityAddMutation additionalVisibility
    ) {
        Value value = toAddAdditionalVisibilityValue(additionalVisibility.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibility.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addAdditionalVisibilityDeleteToMutation(
        Mutation m,
        AdditionalVisibilityDeleteMutation additionalVisibilityDelete
    ) {
        Value value = toDeleteAdditionalVisibilityValue(additionalVisibilityDelete.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibilityDelete.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addMarkPropertyHiddenToMutation(Mutation m, MarkPropertyHiddenData markPropertyHiddenData) {
        Visibility visibility = markPropertyHiddenData.getVisibility();
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(markPropertyHiddenData.getKey(), markPropertyHiddenData.getName(), visibility.getVisibilityString(), getNameSubstitutionStrategy());
        Long timestamp = markPropertyHiddenData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Object data = markPropertyHiddenData.getEventData();
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, columnVisibility, timestamp, toHiddenValue(data));
    }

    public void addMarkPropertyVisibleToMutation(Mutation m, MarkPropertyVisibleData markPropertyVisibleData) {
        Visibility visibility = markPropertyVisibleData.getVisibility();
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(markPropertyVisibleData.getKey(), markPropertyVisibleData.getName(), visibility.getVisibilityString(), getNameSubstitutionStrategy());
        Long timestamp = markPropertyVisibleData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Object data = markPropertyVisibleData.getEventData();
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, columnVisibility, timestamp, toHiddenDeletedValue(data));
    }

    public void addMarkVisibleToMutation(Mutation m, MarkVisibleData markVisibleData) {
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markVisibleData.getVisibility());
        Object data = markVisibleData.getEventData();
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, columnVisibility, toHiddenDeletedValue(data));
    }

    public void addMarkHiddenToMutation(Mutation m, MarkHiddenData markHiddenData) {
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markHiddenData.getVisibility());
        Object data = markHiddenData.getEventData();
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, columnVisibility, toHiddenValue(data));
    }

    public void addSoftDeleteToMutation(Mutation m, SoftDeleteData softDeleteData) {
        Object data = softDeleteData.getEventData();
        Long timestamp = softDeleteData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        m.put(AccumuloElement.CF_SOFT_DELETE, AccumuloElement.CQ_SOFT_DELETE, timestamp, toSoftDeleteDataToValue(data));
    }

    public void addPropertyToMutation(AccumuloGraph graph, Mutation m, String rowKey, Property property) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = transformValue(property.getValue(), rowKey, property);

        // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
        if (graph != null) {
            graph.ensurePropertyDefined(property.getName(), propertyValue);
        }

        Value value = new Value(vertexiumSerializer.objectToBytes(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, property.getTimestamp(), value);
        addPropertyMetadataToMutation(m, property);
    }

    protected abstract NameSubstitutionStrategy getNameSubstitutionStrategy();

    public void addPropertyDeleteToMutation(Mutation m, PropertyDeleteMutation propertyDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertyDelete.getKey(), propertyDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertyDelete.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        addPropertyDeleteMetadataToMutation(m, propertyDelete);
    }

    public void addPropertyMetadataToMutation(Mutation m, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            addPropertyMetadataItemToMutation(
                m,
                property,
                metadataItem.getKey(),
                metadataItem.getValue(),
                metadataItem.getVisibility()
            );
        }
    }

    public void addPropertyMetadataItemToMutation(
        Mutation m,
        Property property,
        String metadataKey,
        Object metadataValue,
        Visibility visibility
    ) {
        Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataKey);
        ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(visibility);
        if (metadataValue == null) {
            addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
        } else {
            addPropertyMetadataItemAddToMutation(m, columnQualifier, metadataVisibility, property.getTimestamp(), metadataValue);
        }
    }

    private void addPropertyMetadataItemAddToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility, long propertyTimestamp, Object value) {
        Value metadataValue = new Value(vertexiumSerializer.objectToBytes(value));
        m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, propertyTimestamp, metadataValue);
    }

    private void addPropertyMetadataItemDeleteToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility) {
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, currentTimeMillis());
    }

    private Text getPropertyMetadataColumnQualifierText(Property property, String metadataKey) {
        String propertyName = property.getName();
        String propertyKey = property.getKey();
        String visibilityString = property.getVisibility().getVisibilityString();
        //noinspection StringBufferReplaceableByString - for speed we use StringBuilder
        StringBuilder keyBuilder = new StringBuilder(propertyName.length() + propertyKey.length() + visibilityString.length() + metadataKey.length());
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyName));
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyKey));
        keyBuilder.append(visibilityString);
        keyBuilder.append(getNameSubstitutionStrategy().deflate(metadataKey));
        String key = keyBuilder.toString();
        Text r = propertyMetadataColumnQualifierTextCache.peek(key);
        if (r == null) {
            r = KeyHelper.getColumnQualifierFromPropertyMetadataColumnQualifier(propertyName, propertyKey, visibilityString, metadataKey, getNameSubstitutionStrategy());
            propertyMetadataColumnQualifierTextCache.put(key, r);
        }
        return r;
    }

    public void addPropertyDeleteMetadataToMutation(Mutation m, PropertyDeleteMutation propertyDeleteMutation) {
        if (propertyDeleteMutation instanceof PropertyPropertyDeleteMutation) {
            Property property = ((PropertyPropertyDeleteMutation) propertyDeleteMutation).getProperty();
            Metadata metadata = property.getMetadata();
            for (Metadata.Entry metadataItem : metadata.entrySet()) {
                Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem.getKey());
                ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
                addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
            }
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        return streamingPropertyValueStorageStrategy.saveStreamingPropertyValue(this, rowKey, property, propertyValue);
    }

    public void addPropertyDeleteToMutation(Mutation m, Property property) {
        checkNotNull(m, "mutation cannot be null");
        checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            Text metadataEntryColumnQualifier = getPropertyMetadataColumnQualifierText(property, metadataEntry.getKey());
            ColumnVisibility metadataEntryVisibility = visibilityToAccumuloVisibility(metadataEntry.getVisibility());
            addPropertyMetadataItemDeleteToMutation(m, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    public void addPropertySoftDeleteToMutation(Mutation m, Property property, long timestamp, Object data) {
        checkNotNull(m, "mutation cannot be null");
        checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, timestamp, toSoftDeleteDataToValue(data));
    }

    public void addPropertySoftDeleteToMutation(Mutation m, PropertySoftDeleteMutation propertySoftDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertySoftDelete.getKey(), propertySoftDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertySoftDelete.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDelete.getTimestamp(), toSoftDeleteDataToValue(propertySoftDelete.getData()));
    }

    public abstract void saveDataMutation(Mutation dataMutation);

    public Value toSoftDeleteDataToValue(Object data) {
        if (data == null) {
            return AccumuloElement.SOFT_DELETE_VALUE;
        }
        // to support un-soft delete in the future prevent the use of values starting with
        byte[] valueDeletedArray = AccumuloElement.SOFT_DELETE_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Soft delete value data cannot start with soft delete value deleted marker: " + AccumuloElement.SOFT_DELETE_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    public Mutation getDeleteRowMutation(String rowKey) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        return m;
    }

    public Mutation getSoftDeleteRowMutation(String rowKey, long timestamp, Object data) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.CF_SOFT_DELETE, AccumuloElement.CQ_SOFT_DELETE, timestamp, toSoftDeleteDataToValue(data));
        return m;
    }

    public Mutation getMarkHiddenRowMutation(String rowKey, ColumnVisibility visibility, Object data) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility, toHiddenValue(data));
        return m;
    }

    public Mutation getMarkVisibleRowMutation(String rowKey, ColumnVisibility visibility, Object data) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility, toHiddenDeletedValue(data));
        return m;
    }

    public Mutation getMarkHiddenPropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility, Object data) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, toHiddenValue(data));
        return m;
    }

    public Mutation getMarkVisiblePropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility, Object data) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, toHiddenDeletedValue(data));
        return m;
    }

    private Value toHiddenDeletedValue(Object data) {
        if (data == null) {
            return AccumuloElement.HIDDEN_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.HIDDEN_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toHiddenValue(Object data) {
        if (data == null) {
            return AccumuloElement.HIDDEN_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.HIDDEN_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Hidden value data cannot start with hidden value deleted marker: " + AccumuloElement.HIDDEN_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private Value toAddAdditionalVisibilityValue(Object data) {
        if (data == null) {
            return AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Add additional visibility value data cannot start with hidden value deleted marker: " + AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private Value toDeleteAdditionalVisibilityValue(Object data) {
        if (data == null) {
            return AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toSignalDeletedValue(Object data) {
        if (data == null) {
            return AccumuloElement.SIGNAL_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.SIGNAL_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toSignalValue(Object data) {
        if (data == null) {
            return ElementMutationBuilder.EMPTY_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.SIGNAL_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Signal value data cannot start with delete value marker: " + AccumuloElement.SIGNAL_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    public Mutation getMarkHiddenOutEdgeMutation(Vertex out, Edge edge, ColumnVisibility columnVisibility, Object data) {
        Mutation m = new Mutation(out.getId());
        m.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenValue(data));
        return m;
    }

    public Mutation getMarkHiddenInEdgeMutation(Vertex in, Edge edge, ColumnVisibility columnVisibility, Object data) {
        Mutation m = new Mutation(in.getId());
        m.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenValue(data));
        return m;
    }

    public Mutation getMarkVisibleOutEdgeMutation(Vertex out, Edge edge, ColumnVisibility columnVisibility, Object data) {
        Mutation m = new Mutation(out.getId());
        m.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenDeletedValue(data));
        return m;
    }

    public Mutation getMarkVisibleInEdgeMutation(Vertex in, Edge edge, ColumnVisibility columnVisibility, Object data) {
        Mutation m = new Mutation(in.getId());
        m.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenDeletedValue(data));
        return m;
    }

    private void queueEvents(
            AccumuloGraph graph,
            Supplier<? extends Element> element,
            ElementMutation<? extends Element> mutation
    ) {
        if (!(mutation instanceof ExistingElementMutation)) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new AddEdgeEvent(graph, (Edge) element.get()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new AddVertexEvent(graph, (Vertex) element.get()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }

        mutation.getProperties().forEach(property ->
            graph.queueEvent(new AddPropertyEvent(graph, element.get(), property))
        );
        mutation.getPropertyDeletes().forEach(propertyDeleteMutation ->
            graph.queueEvent(new DeletePropertyEvent(graph, element.get(), propertyDeleteMutation))
        );
        mutation.getPropertySoftDeletes().forEach(propertySoftDeleteMutation ->
            graph.queueEvent(new SoftDeletePropertyEvent(graph, element.get(), propertySoftDeleteMutation))
        );
        mutation.getAdditionalVisibilities().forEach(additionalVisibilityAddMutation ->
            graph.queueEvent(new AddAdditionalVisibilityEvent(graph, element.get(), additionalVisibilityAddMutation))
        );
        mutation.getAdditionalVisibilityDeletes().forEach(additionalVisibilityDeleteMutation ->
            graph.queueEvent(new DeleteAdditionalVisibilityEvent(graph, element.get(), additionalVisibilityDeleteMutation))
        );
        mutation.getExtendedData().forEach(extendedDataMutation ->
                graph.queueEvent(new AddExtendedDataEvent(
                        graph,
                        element.get(),
                        extendedDataMutation.getTableName(),
                        extendedDataMutation.getRow(),
                        extendedDataMutation.getColumnName(),
                        extendedDataMutation.getKey(),
                        extendedDataMutation.getValue(),
                        extendedDataMutation.getVisibility()
                ))
        );
        mutation.getExtendedDataDeletes().forEach(extendedDataDeleteMutation ->
                graph.queueEvent(new DeleteExtendedDataEvent(
                        graph,
                        element.get(),
                        extendedDataDeleteMutation.getTableName(),
                        extendedDataDeleteMutation.getRow(),
                        extendedDataDeleteMutation.getColumnName(),
                        extendedDataDeleteMutation.getKey()
                ))
        );
        mutation.getAdditionalExtendedDataVisibilities().forEach(additionalExtendedDataVisibility ->
                graph.queueEvent(new AddAdditionalExtendedDataVisibilityEvent(
                        graph,
                        element.get(),
                        additionalExtendedDataVisibility.getTableName(),
                        additionalExtendedDataVisibility.getRow(),
                        additionalExtendedDataVisibility.getAdditionalVisibility()
                ))
        );
        mutation.getAdditionalExtendedDataVisibilityDeletes().forEach(additionalExtendedDataVisibilityDelete ->
                graph.queueEvent(new DeleteAdditionalExtendedDataVisibilityEvent(
                        graph,
                        element.get(),
                        additionalExtendedDataVisibilityDelete.getTableName(),
                        additionalExtendedDataVisibilityDelete.getRow(),
                        additionalExtendedDataVisibilityDelete.getAdditionalVisibility()
                ))
        );
        mutation.getMarkPropertyHiddenData().forEach(markPropertyHiddenData ->
            graph.queueEvent(new MarkHiddenPropertyEvent(
                    graph,
                    element.get(),
                    markPropertyHiddenData.getKey(),
                    markPropertyHiddenData.getName(),
                    markPropertyHiddenData.getPropertyVisibility(),
                    markPropertyHiddenData.getTimestamp(),
                    markPropertyHiddenData.getVisibility(),
                    markPropertyHiddenData.getEventData()
            ))
        );
        mutation.getMarkPropertyVisibleData().forEach(markPropertyVisibleData ->
            graph.queueEvent(new MarkVisiblePropertyEvent(
                    graph,
                    element.get(),
                    markPropertyVisibleData.getKey(),
                    markPropertyVisibleData.getName(),
                    markPropertyVisibleData.getPropertyVisibility(),
                    markPropertyVisibleData.getTimestamp(),
                    markPropertyVisibleData.getVisibility(),
                    markPropertyVisibleData.getEventData()
            ))
        );
        mutation.getMarkHiddenData().forEach(markHiddenData -> {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new MarkHiddenEdgeEvent(graph, (Edge) element.get(), markHiddenData.getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new MarkHiddenVertexEvent(graph, (Vertex) element.get(), markHiddenData.getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        });
        mutation.getMarkVisibleData().forEach(markVisibleData -> {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new MarkVisibleEdgeEvent(graph, (Edge) element.get(), markVisibleData.getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new MarkVisibleVertexEvent(graph, (Vertex) element.get(), markVisibleData.getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        });

        if (mutation.getSoftDeleteData() != null) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new SoftDeleteEdgeEvent(graph, (Edge) element.get(), mutation.getSoftDeleteData().getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new SoftDeleteVertexEvent(graph, (Vertex) element.get(), mutation.getSoftDeleteData().getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }

        if (mutation.isDeleteElement()) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new DeleteEdgeEvent(graph, (Edge) element.get()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new DeleteVertexEvent(graph, (Vertex) element.get()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }
    }
}
