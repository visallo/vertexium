package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertyPropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.vertexium.util.IncreasingTime.currentTimeMillis;
import static org.vertexium.util.StreamUtils.stream;

public abstract class ElementMutationBuilder {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElementMutationBuilder.class);
    public static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value("".getBytes());

    private final FileSystem fileSystem;
    private final VertexiumSerializer vertexiumSerializer;
    private final long maxStreamingPropertyValueTableDataSize;
    private final String dataDir;
    private static final Cache<String, Text> propertyMetadataColumnQualifierTextCache = CacheBuilder
            .newCache(String.class, Text.class)
            .name(ElementMutationBuilder.class, "propertyMetadataColumnQualifierTextCache")
            .maxSize(10000)
            .build();

    protected ElementMutationBuilder(FileSystem fileSystem, VertexiumSerializer vertexiumSerializer, long maxStreamingPropertyValueTableDataSize, String dataDir) {
        this.fileSystem = fileSystem;
        this.vertexiumSerializer = vertexiumSerializer;
        this.maxStreamingPropertyValueTableDataSize = maxStreamingPropertyValueTableDataSize;
        this.dataDir = dataDir;
    }

    public void saveVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        Mutation m = createMutationForVertexBuilder(graph, vertexBuilder, timestamp);
        saveVertexMutation(m);
        saveExtendedDataMutations(graph, ElementType.VERTEX, vertexBuilder);
    }

    private void saveExtendedDataMutations(AccumuloGraph graph, ElementType elementType, ElementBuilder elementBuilder) {
        if (elementBuilder.getExtendedData() == null) {
            return;
        }

        Iterable<ExtendedDataMutation> extendedData = elementBuilder.getExtendedData();
        saveExtendedData(graph, elementBuilder.getElementId(), elementType, extendedData);
    }

    void saveExtendedData(AccumuloGraph graph, String elementId, ElementType elementType, Iterable<ExtendedDataMutation> extendedData) {
        Map<String, List<ExtendedDataMutation>> extendedDatasByTableName = stream(extendedData)
                .collect(Collectors.groupingBy(edm -> edm.getTableName() + edm.getRow()));
        for (Map.Entry<String, List<ExtendedDataMutation>> entry : extendedDatasByTableName.entrySet()) {
            List<ExtendedDataMutation> mutationsForTableAndRow = entry.getValue();
            String tableName = mutationsForTableAndRow.get(0).getTableName();
            String row = mutationsForTableAndRow.get(0).getRow();

            Mutation m = new Mutation(KeyHelper.createExtendedDataRowKey(elementType, elementId, tableName, row));
            for (ExtendedDataMutation edm : mutationsForTableAndRow) {
                Object value = transformValue(edm.getValue(), null, null);

                // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
                if (graph != null) {
                    graph.ensurePropertyDefined(edm.getColumnName(), value);
                }

                m.put(
                        AccumuloElement.CF_EXTENDED_DATA,
                        new Text(edm.getColumnName()),
                        visibilityToAccumuloVisibility(edm.getVisibility()),
                        new Value(vertexiumSerializer.objectToBytes(value))
                );
            }
            saveExtendedDataMutation(elementType, m);
        }
    }

    protected abstract void saveExtendedDataMutation(ElementType elementType, Mutation m);

    protected abstract void saveVertexMutation(Mutation m);

    private Mutation createMutationForVertexBuilder(AccumuloGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        String vertexRowKey = vertexBuilder.getElementId();
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
        Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem);
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
        results.add(new KeyValuePair(new Key(elementRowKey, AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDeleteMutation.getTimestamp()), AccumuloElement.SOFT_DELETE_VALUE));
    }

    public void saveEdgeBuilder(AccumuloGraph graph, EdgeBuilderBase edgeBuilder, long timestamp) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edgeBuilder.getVisibility());
        Mutation m = createMutationForEdgeBuilder(graph, edgeBuilder, edgeColumnVisibility, timestamp);
        saveEdgeMutation(m);

        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getLabel();
        saveEdgeInfoOnVertex(
                edgeBuilder.getElementId(),
                edgeBuilder.getOutVertexId(),
                edgeBuilder.getInVertexId(),
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
        String edgeRowKey = edgeBuilder.getElementId();
        Mutation m = new Mutation(edgeRowKey);
        String edgeLabel = edgeBuilder.getLabel();
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edgeLabel = edgeBuilder.getNewEdgeLabel();
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edgeBuilder.getLabel()), edgeColumnVisibility, currentTimeMillis());
        }
        m.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getOutVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getInVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        createMutationForElementBuilder(graph, edgeBuilder, edgeRowKey, m);
        return m;
    }

    private Mutation createAlterEdgeLabelMutation(Edge edge, String newEdgeLabel, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = edge.getId();
        Mutation m = new Mutation(edgeRowKey);
        m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), edgeColumnVisibility, currentTimeMillis());
        m.put(AccumuloEdge.CF_SIGNAL, new Text(newEdgeLabel), edgeColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        return m;
    }

    public boolean alterElementVisibility(Mutation m, AccumuloElement element, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(element.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }

        if (element instanceof AccumuloEdge) {
            AccumuloEdge edge = (AccumuloEdge) element;
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof AccumuloVertex) {
            m.putDelete(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
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
            addPropertyMetadataItemToMutation(m, property, metadataItem);
        }
    }

    private void addPropertyMetadataItemToMutation(Mutation m, Property property, Metadata.Entry metadataItem) {
        Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem);
        ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
        if (metadataItem.getValue() == null) {
            addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
        } else {
            addPropertyMetadataItemAddToMutation(m, columnQualifier, metadataVisibility, property.getTimestamp(), metadataItem.getValue());
        }
    }

    private void addPropertyMetadataItemAddToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility, long propertyTimestamp, Object value) {
        Value metadataValue = new Value(vertexiumSerializer.objectToBytes(value));
        m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, propertyTimestamp, metadataValue);
    }

    private void addPropertyMetadataItemDeleteToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility) {
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, currentTimeMillis());
    }

    private Text getPropertyMetadataColumnQualifierText(Property property, Metadata.Entry metadataItem) {
        final String propertyName = property.getName();
        final String propertyKey = property.getKey();
        final String visibilityString = property.getVisibility().getVisibilityString();
        final String metadataKey = metadataItem.getKey();
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
                Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem);
                ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
                addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
            }
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem, this.dataDir, rowKey, property);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                IOUtils.copy(propertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            if (out.hasExceededSizeLimit()) {
                LOGGER.debug("saved large file to \"%s\" (length: %d)", largeDataStore.getFullHdfsPath(), out.getLength());
                return new StreamingPropertyValueHdfsRef(largeDataStore.getRelativeFileName(), propertyValue);
            } else {
                return saveStreamingPropertyValueSmall(rowKey, property, out.getSmall(), propertyValue);
            }
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    public void addPropertyDeleteToMutation(Mutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            Text metadataEntryColumnQualifier = getPropertyMetadataColumnQualifierText(property, metadataEntry);
            ColumnVisibility metadataEntryVisibility = visibilityToAccumuloVisibility(metadataEntry.getVisibility());
            addPropertyMetadataItemDeleteToMutation(m, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    public void addPropertySoftDeleteToMutation(Mutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, currentTimeMillis(), AccumuloElement.SOFT_DELETE_VALUE);
    }

    public void addPropertySoftDeleteToMutation(Mutation m, PropertySoftDeleteMutation propertySoftDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertySoftDelete.getKey(), propertySoftDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertySoftDelete.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDelete.getTimestamp(), AccumuloElement.SOFT_DELETE_VALUE);
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(String rowKey, Property property, byte[] data, StreamingPropertyValue propertyValue) {
        String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
        Mutation dataMutation = new Mutation(dataTableRowKey);
        dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, new Value(data));
        saveDataMutation(dataMutation);
        return new StreamingPropertyValueTableRef(dataTableRowKey, propertyValue, data);
    }

    protected abstract void saveDataMutation(Mutation dataMutation);
}
