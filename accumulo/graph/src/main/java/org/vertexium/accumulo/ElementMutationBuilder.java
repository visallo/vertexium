package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.vertexium.*;
import org.vertexium.accumulo.models.AccumuloEdgeInfo;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.vertexium.util.IncreasingTime.currentTimeMillis;

public abstract class ElementMutationBuilder {
    public static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value("".getBytes());

    private final MetadataPlugin metadataPlugin;
    private final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    private final VertexiumSerializer vertexiumSerializer;
    private static final Cache<String, Text> propertyMetadataColumnQualifierTextCache = Cache2kBuilder.of(String.class, Text.class)
        .name(ElementMutationBuilder.class, "propertyMetadataColumnQualifierTextCache")
        .entryCapacity(10000)
        .eternal(true)
        .build();

    protected ElementMutationBuilder(
        MetadataPlugin metadataPlugin,
        StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy,
        VertexiumSerializer vertexiumSerializer
    ) {
        this.metadataPlugin = metadataPlugin;
        this.streamingPropertyValueStorageStrategy = streamingPropertyValueStorageStrategy;
        this.vertexiumSerializer = vertexiumSerializer;
    }

    public CompletableFuture<Void> saveVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        CompletableMutation m = createMutationForVertexBuilder(graph, vertexBuilder, timestamp);
        return CompletableFuture.allOf(
            saveVertexMutation(m),
            saveExtendedDataMutations(graph, ElementType.VERTEX, vertexBuilder)
        );
    }

    private <T extends Element> CompletableFuture<Void> saveExtendedDataMutations(AccumuloGraph graph, ElementType elementType, ElementBuilder<T> elementBuilder) {
        return saveExtendedData(
            graph,
            elementBuilder.getId(),
            elementType,
            elementBuilder.getExtendedData(),
            elementBuilder.getExtendedDataDeletes(),
            elementBuilder.getAdditionalExtendedDataVisibilities(),
            elementBuilder.getAdditionalExtendedDataVisibilityDeletes()
        );
    }

    CompletableFuture<Void> saveExtendedData(
        AccumuloGraph graph,
        String elementId,
        ElementType elementType,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
    ) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
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

                CompletableMutation m = new CompletableMutation(KeyHelper.createExtendedDataRowKey(elementType, elementId, tableName, row));

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

                futures.add(saveExtendedDataMutation(elementType, m));
            }
        }
        return CompletableFutureUtils.allOf(futures);
    }

    protected abstract CompletableFuture<Void> saveExtendedDataMutation(ElementType elementType, CompletableMutation m);

    protected abstract CompletableFuture<Void> saveVertexMutation(CompletableMutation m);

    private CompletableMutation createMutationForVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        String vertexRowKey = vertexBuilder.getId();
        CompletableMutation m = new CompletableMutation(vertexRowKey);
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, visibilityToAccumuloVisibility(vertexBuilder.getVisibility()), timestamp, EMPTY_VALUE);
        createMutationForElementBuilder(graph, vertexBuilder, vertexRowKey, m);
        return m;
    }

    private <T extends Element> void createMutationForElementBuilder(AccumuloGraph graph, ElementBuilder<T> elementBuilder, String rowKey, CompletableMutation m) {
        for (PropertyDeleteMutation propertyDeleteMutation : elementBuilder.getPropertyDeletes()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : elementBuilder.getPropertySoftDeletes()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
        }
        for (Property property : elementBuilder.getProperties()) {
            addPropertyToMutation(graph, m, elementBuilder, rowKey, property);
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : elementBuilder.getAdditionalVisibilities()) {
            addAdditionalVisibilityToMutation(m, additionalVisibility);
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : elementBuilder.getAdditionalVisibilityDeletes()) {
            addAdditionalVisibilityDeleteToMutation(m, additionalVisibilityDelete);
        }
        for (MarkPropertyHiddenMutation markPropertyHidden : elementBuilder.getMarkPropertyHiddenMutations()) {
            addMarkPropertyHiddenToMutation(m, markPropertyHidden);
        }
        for (MarkPropertyVisibleMutation markPropertyVisible : elementBuilder.getMarkPropertyVisibleMutations()) {
            addMarkPropertyVisibleToMutation(m, markPropertyVisible);
        }
        Iterable<ExtendedDataMutation> extendedData = elementBuilder.getExtendedData();
        saveExtendedDataMarkers(m, extendedData);
    }

    public void addMarkPropertyVisibleToMutation(CompletableMutation m, MarkPropertyVisibleMutation markPropertyVisible) {
        Long timestamp = markPropertyVisible.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(
            markPropertyVisible.getPropertyKey(),
            markPropertyVisible.getPropertyName(),
            markPropertyVisible.getPropertyVisibility().toString(),
            getNameSubstitutionStrategy()
        );
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markPropertyVisible.getVisibility());
        m.put(
            AccumuloElement.CF_PROPERTY_HIDDEN,
            columnQualifier,
            columnVisibility,
            timestamp,
            toHiddenDeletedValue(markPropertyVisible.getEventData())
        );
    }

    public void addMarkPropertyHiddenToMutation(CompletableMutation m, MarkPropertyHiddenMutation markPropertyHidden) {
        Long timestamp = markPropertyHidden.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(
            markPropertyHidden.getPropertyKey(),
            markPropertyHidden.getPropertyName(),
            markPropertyHidden.getPropertyVisibility().toString(),
            getNameSubstitutionStrategy()
        );
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markPropertyHidden.getVisibility());
        m.put(
            AccumuloElement.CF_PROPERTY_HIDDEN,
            columnQualifier,
            columnVisibility,
            timestamp,
            toHiddenValue(markPropertyHidden.getEventData())
        );
    }

    public CompletableFuture<Void> saveExtendedDataMarkers(
        String elementId,
        ElementType elementType,
        Iterable<ExtendedDataMutation> extendedData
    ) {
        Set<TableNameVisibilityPair> uniquePairs = TableNameVisibilityPair.getUniquePairs(extendedData);
        if (uniquePairs.size() == 0) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableMutation m = new CompletableMutation(elementId);
        for (TableNameVisibilityPair pair : uniquePairs) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
        return saveElementMutation(elementType, m);
    }

    private CompletableFuture<Void> saveElementMutation(ElementType elementType, CompletableMutation m) {
        switch (elementType) {
            case VERTEX:
                return saveVertexMutation(m);
            case EDGE:
                return saveEdgeMutation(m);
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    private void saveExtendedDataMarkers(CompletableMutation m, Iterable<ExtendedDataMutation> extendedData) {
        for (TableNameVisibilityPair pair : TableNameVisibilityPair.getUniquePairs(extendedData)) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
    }

    private void addExtendedDataMarkerToElementMutation(CompletableMutation m, TableNameVisibilityPair pair) {
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

        // out vertex.
        Text vertexOutIdRowKey = new Text(edge.getVertexId(Direction.OUT));
        AccumuloEdgeInfo edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.IN));
        results.add(new KeyValuePair(new Key(vertexOutIdRowKey, AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp), edgeInfo.toValue()));

        // in vertex.
        Text vertexInIdRowKey = new Text(edge.getVertexId(Direction.IN));
        edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.OUT));
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

    public CompletableFuture<Void> saveEdgeBuilder(AccumuloGraph graph, EdgeBuilderBase edgeBuilder, long timestamp) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edgeBuilder.getVisibility());
        CompletableMutation m = createMutationForEdgeBuilder(graph, edgeBuilder, edgeColumnVisibility, timestamp);
        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getEdgeLabel();

        return CompletableFuture.allOf(
            saveEdgeMutation(m),
            saveEdgeInfoOnVertex(
                edgeBuilder.getId(),
                edgeBuilder.getVertexId(Direction.OUT),
                edgeBuilder.getVertexId(Direction.IN),
                edgeLabel,
                edgeColumnVisibility
            ),
            saveExtendedDataMutations(graph, ElementType.EDGE, edgeBuilder)
        );
    }

    private CompletableFuture<Void> saveEdgeInfoOnVertex(String edgeId, String outVertexId, String inVertexId, String edgeLabel, ColumnVisibility edgeColumnVisibility) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Text edgeIdText = new Text(edgeId);

        // Update out vertex.
        CompletableMutation addEdgeToOutMutation = new CompletableMutation(outVertexId);
        AccumuloEdgeInfo edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), inVertexId);
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());
        futures.add(saveVertexMutation(addEdgeToOutMutation));

        // Update in vertex.
        CompletableMutation addEdgeToInMutation = new CompletableMutation(inVertexId);
        edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), outVertexId);
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());
        futures.add(saveVertexMutation(addEdgeToInMutation));

        return CompletableFutureUtils.allOf(futures);
    }

    public CompletableFuture<Void> alterEdgeLabel(Edge edge, String newEdgeLabel) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        CompletableMutation m = createAlterEdgeLabelMutation(edge, newEdgeLabel, edgeColumnVisibility);

        futures.add(saveEdgeMutation(m));

        futures.add(saveEdgeInfoOnVertex(
            edge.getId(),
            edge.getVertexId(Direction.OUT),
            edge.getVertexId(Direction.IN),
            newEdgeLabel,
            edgeColumnVisibility
        ));

        return CompletableFutureUtils.allOf(futures);
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract CompletableFuture<Void> saveEdgeMutation(CompletableMutation m);

    private CompletableMutation createMutationForEdgeBuilder(AccumuloGraph graph, EdgeBuilderBase edgeBuilder, ColumnVisibility edgeColumnVisibility, long timestamp) {
        String edgeRowKey = edgeBuilder.getId();
        CompletableMutation m = new CompletableMutation(edgeRowKey);
        String edgeLabel = edgeBuilder.getEdgeLabel();
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edgeLabel = edgeBuilder.getNewEdgeLabel();
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edgeBuilder.getEdgeLabel()), edgeColumnVisibility, currentTimeMillis());
        }
        m.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getVertexId(Direction.OUT)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getVertexId(Direction.IN)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        createMutationForElementBuilder(graph, edgeBuilder, edgeRowKey, m);
        return m;
    }

    private CompletableMutation createAlterEdgeLabelMutation(Edge edge, String newEdgeLabel, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = edge.getId();
        CompletableMutation m = new CompletableMutation(edgeRowKey);
        m.put(AccumuloEdge.CF_SIGNAL, new Text(newEdgeLabel), edgeColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        return m;
    }

    public boolean alterElementVisibility(CompletableMutation m, AccumuloElement element, Visibility newVisibility, Object data) {
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

    public boolean alterEdgeVertexOutVertex(CompletableMutation vertexOutMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        AccumuloEdgeInfo edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.IN));
        vertexOutMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public boolean alterEdgeVertexInVertex(CompletableMutation vertexInMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        AccumuloEdgeInfo edgeInfo = new AccumuloEdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.OUT));
        vertexInMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addAdditionalVisibilityToMutation(
        CompletableMutation m,
        AdditionalVisibilityAddMutation additionalVisibility
    ) {
        Value value = toAddAdditionalVisibilityValue(additionalVisibility.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibility.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addAdditionalVisibilityDeleteToMutation(
        CompletableMutation m,
        AdditionalVisibilityDeleteMutation additionalVisibilityDelete
    ) {
        Value value = toDeleteAdditionalVisibilityValue(additionalVisibilityDelete.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibilityDelete.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addPropertyToMutation(AccumuloGraph graph, CompletableMutation m, ElementId elementId, String rowKey, Property property) {
        addPropertyToMutation(graph, m, elementId, rowKey, property, property.getValue());
    }

    public void addPropertyToMutation(
        AccumuloGraph graph,
        CompletableMutation m,
        ElementId elementId,
        String rowKey,
        Property property,
        Object propertyValue
    ) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object newPropertyValue = transformValue(propertyValue, rowKey, property);

        // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
        if (graph != null) {
            graph.ensurePropertyDefined(property.getName(), newPropertyValue);
        }

        Value value = new Value(vertexiumSerializer.objectToBytes(newPropertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, property.getTimestamp(), value);
        addPropertyMetadataToMutation(m, elementId, property);
    }

    protected abstract NameSubstitutionStrategy getNameSubstitutionStrategy();

    public void addPropertyDeleteToMutation(CompletableMutation m, PropertyDeleteMutation propertyDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertyDelete.getKey(), propertyDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertyDelete.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        addPropertyDeleteMetadataToMutation(m, propertyDelete);
    }

    public void addPropertyMetadataToMutation(CompletableMutation m, ElementId elementId, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            addPropertyMetadataItemToMutation(
                m,
                elementId,
                property,
                metadataItem.getKey(),
                metadataItem.getValue(),
                metadataItem.getVisibility()
            );
        }
    }

    public void addPropertyMetadataItemToMutation(
        CompletableMutation m,
        ElementId elementId,
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
            addPropertyMetadataItemAddToMutation(
                m,
                elementId,
                property,
                columnQualifier,
                metadataVisibility,
                metadataKey,
                visibility,
                metadataValue
            );
        }
    }

    private void addPropertyMetadataItemAddToMutation(
        CompletableMutation m,
        ElementId elementId,
        Property property,
        Text columnQualifier,
        ColumnVisibility metadataVisibility,
        String metadataKey,
        Visibility visibility,
        Object value
    ) {
        if (!metadataPlugin.shouldWriteMetadata(elementId, property, metadataKey, visibility, value, property.getTimestamp())) {
            return;
        }
        Value metadataValue = new Value(vertexiumSerializer.objectToBytes(value));
        m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, property.getTimestamp(), metadataValue);
    }

    private void addPropertyMetadataItemDeleteToMutation(CompletableMutation m, Text columnQualifier, ColumnVisibility metadataVisibility) {
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
        return propertyMetadataColumnQualifierTextCache.computeIfAbsent(
            key,
            () -> KeyHelper.getColumnQualifierFromPropertyMetadataColumnQualifier(
                propertyName,
                propertyKey,
                visibilityString,
                metadataKey,
                getNameSubstitutionStrategy()
            )
        );
    }

    public void addPropertyDeleteMetadataToMutation(CompletableMutation m, PropertyDeleteMutation propertyDeleteMutation) {
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

    public void addPropertyDeleteToMutation(CompletableMutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            Text metadataEntryColumnQualifier = getPropertyMetadataColumnQualifierText(property, metadataEntry.getKey());
            ColumnVisibility metadataEntryVisibility = visibilityToAccumuloVisibility(metadataEntry.getVisibility());
            addPropertyMetadataItemDeleteToMutation(m, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    public void addPropertySoftDeleteToMutation(CompletableMutation m, Property property, long timestamp, Object data) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, timestamp, toSoftDeleteDataToValue(data));
    }

    public void addPropertySoftDeleteToMutation(CompletableMutation m, PropertySoftDeleteMutation propertySoftDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertySoftDelete.getKey(), propertySoftDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertySoftDelete.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDelete.getTimestamp(), toSoftDeleteDataToValue(propertySoftDelete.getData()));
    }

    public abstract CompletableFuture<Void> saveDataMutation(CompletableMutation dataMutation);

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

    public CompletableMutation getDeleteRowMutation(String rowKey) {
        CompletableMutation m = new CompletableMutation(rowKey);
        m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        return m;
    }

    public CompletableMutation getSoftDeleteRowMutation(String rowKey, long timestamp, Object data) {
        CompletableMutation m = new CompletableMutation(rowKey);
        m.put(AccumuloElement.CF_SOFT_DELETE, AccumuloElement.CQ_SOFT_DELETE, timestamp, toSoftDeleteDataToValue(data));
        return m;
    }

    public CompletableMutation getMarkHiddenRowMutation(String rowKey, ColumnVisibility visibility, Object data) {
        CompletableMutation m = new CompletableMutation(rowKey);
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility, toHiddenValue(data));
        return m;
    }

    public CompletableMutation getMarkVisibleRowMutation(String rowKey, ColumnVisibility visibility, Object data) {
        CompletableMutation m = new CompletableMutation(rowKey);
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility, toHiddenDeletedValue(data));
        return m;
    }

    public CompletableMutation getMarkHiddenPropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility, Object data) {
        CompletableMutation m = new CompletableMutation(rowKey);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, toHiddenValue(data));
        return m;
    }

    public CompletableMutation getMarkVisiblePropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility, Object data) {
        CompletableMutation m = new CompletableMutation(rowKey);
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

    public CompletableMutation getMarkHiddenOutEdgeMutation(Vertex out, Edge edge, ColumnVisibility columnVisibility, Object data) {
        CompletableMutation m = new CompletableMutation(out.getId());
        m.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenValue(data));
        return m;
    }

    public CompletableMutation getMarkHiddenInEdgeMutation(Vertex in, Edge edge, ColumnVisibility columnVisibility, Object data) {
        CompletableMutation m = new CompletableMutation(in.getId());
        m.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenValue(data));
        return m;
    }

    public CompletableMutation getMarkVisibleOutEdgeMutation(Vertex out, Edge edge, ColumnVisibility columnVisibility, Object data) {
        CompletableMutation m = new CompletableMutation(out.getId());
        m.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenDeletedValue(data));
        return m;
    }

    public CompletableMutation getMarkVisibleInEdgeMutation(Vertex in, Edge edge, ColumnVisibility columnVisibility, Object data) {
        CompletableMutation m = new CompletableMutation(in.getId());
        m.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, toHiddenDeletedValue(data));
        return m;
    }
}
