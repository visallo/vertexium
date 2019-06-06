package org.vertexium.accumulo;

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
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.ArrayUtils;
import org.vertexium.util.ExtendedDataMutationUtils;
import org.vertexium.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void saveVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        Mutation m = createMutationForVertexBuilder(graph, vertexBuilder, timestamp);
        saveVertexMutation(m);
        saveExtendedDataMutations(graph, ElementType.VERTEX, vertexBuilder);
    }

    private <T extends Element> void saveExtendedDataMutations(AccumuloGraph graph, ElementType elementType, ElementBuilder<T> elementBuilder) {
        saveExtendedData(
            graph,
            elementBuilder.getId(),
            elementType,
            elementBuilder.getExtendedData(),
            elementBuilder.getExtendedDataDeletes(),
            elementBuilder.getAdditionalExtendedDataVisibilities(),
            elementBuilder.getAdditionalExtendedDataVisibilityDeletes()
        );
    }

    void saveExtendedData(
        AccumuloGraph graph,
        String elementId,
        ElementType elementType,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
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

                Mutation m = new Mutation(KeyHelper.createExtendedDataRowKey(elementType, elementId, tableName, row));

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

                saveExtendedDataMutation(elementType, m);
            }
        }
    }

    protected abstract void saveExtendedDataMutation(ElementType elementType, Mutation m);

    protected abstract void saveVertexMutation(Mutation m);

    private Mutation createMutationForVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        String vertexRowKey = vertexBuilder.getId();
        Mutation m = new Mutation(vertexRowKey);
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, visibilityToAccumuloVisibility(vertexBuilder.getVisibility()), timestamp, EMPTY_VALUE);
        createMutationForElementBuilder(graph, vertexBuilder, vertexRowKey, m);
        return m;
    }

    private <T extends Element> void createMutationForElementBuilder(AccumuloGraph graph, ElementBuilder<T> elementBuilder, String rowKey, Mutation m) {
        for (PropertyDeleteMutation propertyDeleteMutation : elementBuilder.getPropertyDeletes()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : elementBuilder.getPropertySoftDeletes()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
        }
        for (Property property : elementBuilder.getProperties()) {
            addPropertyToMutation(graph, m, rowKey, property);
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : elementBuilder.getAdditionalVisibilities()) {
            addAdditionalVisibilityToMutation(m, additionalVisibility);
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : elementBuilder.getAdditionalVisibilityDeletes()) {
            addAdditionalVisibilityDeleteToMutation(m, additionalVisibilityDelete);
        }
        Iterable<ExtendedDataMutation> extendedData = elementBuilder.getExtendedData();
        saveExtendedDataMarkers(m, extendedData);
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
                saveVertexMutation(m);
                break;
            case EDGE:
                saveEdgeMutation(m);
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

        // out vertex.
        Text vertexOutIdRowKey = new Text(edge.getVertexId(Direction.OUT));
        org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.IN));
        results.add(new KeyValuePair(new Key(vertexOutIdRowKey, AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp), edgeInfo.toValue()));

        // in vertex.
        Text vertexInIdRowKey = new Text(edge.getVertexId(Direction.IN));
        edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edge.getVertexId(Direction.OUT));
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

    public void saveEdgeBuilder(AccumuloGraph graph, EdgeBuilderBase edgeBuilder, long timestamp) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edgeBuilder.getVisibility());
        Mutation m = createMutationForEdgeBuilder(graph, edgeBuilder, edgeColumnVisibility, timestamp);
        saveEdgeMutation(m);

        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getEdgeLabel();
        saveEdgeInfoOnVertex(
            edgeBuilder.getId(),
            edgeBuilder.getVertexId(Direction.OUT),
            edgeBuilder.getVertexId(Direction.IN),
            edgeLabel,
            edgeColumnVisibility
        );

        saveExtendedDataMutations(graph, ElementType.EDGE, edgeBuilder);
    }

    private void saveEdgeInfoOnVertex(String edgeId, String outVertexId, String inVertexId, String edgeLabel, ColumnVisibility edgeColumnVisibility) {
        Text edgeIdText = new Text(edgeId);

        // Update out vertex.
        Mutation addEdgeToOutMutation = new Mutation(outVertexId);
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), inVertexId);
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToOutMutation);

        // Update in vertex.
        Mutation addEdgeToInMutation = new Mutation(inVertexId);
        edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), outVertexId);
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToInMutation);
    }

    public void alterEdgeLabel(Edge edge, String newEdgeLabel) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        Mutation m = createAlterEdgeLabelMutation(edge, newEdgeLabel, edgeColumnVisibility);
        saveEdgeMutation(m);

        saveEdgeInfoOnVertex(
            edge.getId(),
            edge.getVertexId(Direction.OUT),
            edge.getVertexId(Direction.IN),
            newEdgeLabel,
            edgeColumnVisibility
        );
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutation(Mutation m);

    private Mutation createMutationForEdgeBuilder(AccumuloGraph graph, EdgeBuilderBase edgeBuilder, ColumnVisibility edgeColumnVisibility, long timestamp) {
        String edgeRowKey = edgeBuilder.getId();
        Mutation m = new Mutation(edgeRowKey);
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

    private Mutation createAlterEdgeLabelMutation(Edge edge, String newEdgeLabel, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = edge.getId();
        Mutation m = new Mutation(edgeRowKey);
        m.put(AccumuloEdge.CF_SIGNAL, new Text(newEdgeLabel), edgeColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        return m;
    }

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
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.IN));
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
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.OUT));
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

    public void addPropertySoftDeleteToMutation(Mutation m, Property property, long timestamp, Object data) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
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
}
