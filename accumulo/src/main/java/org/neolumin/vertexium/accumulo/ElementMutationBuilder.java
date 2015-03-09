package org.neolumin.vertexium.accumulo;

import com.google.common.base.Joiner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;
import org.neolumin.vertexium.mutation.PropertyPropertyRemoveMutation;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;
import org.neolumin.vertexium.property.StreamingPropertyValue;
import org.neolumin.vertexium.util.LimitOutputStream;
import org.neolumin.vertexium.util.Preconditions;
import org.neolumin.vertexium.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class ElementMutationBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementMutationBuilder.class);
    private static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value(new byte[0]);
    public static final String VALUE_SEPARATOR = "\u001f";

    private final FileSystem fileSystem;
    private final ValueSerializer valueSerializer;
    private final long maxStreamingPropertyValueTableDataSize;
    private final String dataDir;

    protected ElementMutationBuilder(FileSystem fileSystem, ValueSerializer valueSerializer, long maxStreamingPropertyValueTableDataSize, String dataDir) {
        this.fileSystem = fileSystem;
        this.valueSerializer = valueSerializer;
        this.maxStreamingPropertyValueTableDataSize = maxStreamingPropertyValueTableDataSize;
        this.dataDir = dataDir;
    }

    public void saveVertex(AccumuloVertex vertex) {
        Mutation m = createMutationForVertex(vertex);
        saveVertexMutation(m);
    }

    protected abstract void saveVertexMutation(Mutation m);

    private Mutation createMutationForVertex(AccumuloVertex vertex) {
        String vertexRowKey = AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId();
        Mutation m = new Mutation(vertexRowKey);
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, visibilityToAccumuloVisibility(vertex.getVisibility()), EMPTY_VALUE);
        for (PropertyRemoveMutation propertyRemoveMutation : vertex.getPropertyRemoveMutations()) {
            addPropertyRemoveToMutation(m, propertyRemoveMutation);
        }
        for (Property property : vertex.getProperties()) {
            addPropertyToMutation(m, vertexRowKey, property);
        }
        return m;
    }

    public void saveEdge(AccumuloEdge edge) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        Mutation m = createMutationForEdge(edge, edgeColumnVisibility);
        saveEdgeMutation(m);

        String edgeLabel = edge.getNewEdgeLabel() != null ? edge.getNewEdgeLabel() : edge.getLabel();
        saveEdgeInfoOnVertex(edge, edgeLabel, edgeColumnVisibility);
    }

    private void saveEdgeInfoOnVertex(AccumuloEdge edge, String edgeLabel, ColumnVisibility edgeColumnVisibility) {
        // Update out vertex.
        Mutation addEdgeToOutMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.OUT));
        EdgeInfo edgeInfo = new EdgeInfo(edgeLabel, edge.getVertexId(Direction.IN));
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToOutMutation);

        // Update in vertex.
        Mutation addEdgeToInMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.IN));
        edgeInfo = new EdgeInfo(edgeLabel, edge.getVertexId(Direction.OUT));
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToInMutation);
    }

    public void alterEdgeLabel(AccumuloEdge edge, String newEdgeLabel) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        Mutation m = createAlterEdgeLabelMutation(edge, newEdgeLabel, edgeColumnVisibility);
        saveEdgeMutation(m);

        saveEdgeInfoOnVertex(edge, newEdgeLabel, edgeColumnVisibility);
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutation(Mutation m);

    private Mutation createMutationForEdge(AccumuloEdge edge, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId();
        Mutation m = new Mutation(edgeRowKey);
        String edgeLabel = edge.getLabel();
        if (edge.getNewEdgeLabel() != null) {
            edgeLabel = edge.getNewEdgeLabel();
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), edgeColumnVisibility);
        }
        m.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        for (PropertyRemoveMutation propertyRemoveMutation : edge.getPropertyRemoveMutations()) {
            addPropertyRemoveToMutation(m, propertyRemoveMutation);
        }
        for (Property property : edge.getProperties()) {
            addPropertyToMutation(m, edgeRowKey, property);
        }
        return m;
    }

    private Mutation createAlterEdgeLabelMutation(AccumuloEdge edge, String newEdgeLabel, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId();
        Mutation m = new Mutation(edgeRowKey);
        m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), edgeColumnVisibility);
        m.put(AccumuloEdge.CF_SIGNAL, new Text(newEdgeLabel), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
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
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility);
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), currentColumnVisibility);
            m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), currentColumnVisibility);
            m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof AccumuloVertex) {
            m.putDelete(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility);
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else {
            throw new IllegalArgumentException("Invalid element type: " + element);
        }
        return true;
    }

    public boolean alterEdgeVertexOutVertex(Mutation mvout, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN));
        mvout.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), currentColumnVisibility);
        mvout.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public boolean alterEdgeVertexInVertex(Mutation mvin, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT));
        mvin.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        mvin.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addPropertyToMutation(Mutation m, String rowKey, Property property) {
        Text columnQualifier = getPropertyColumnQualifier(property);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        Value value = new Value(valueSerializer.objectToValue(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, value);
        addPropertyMetadataToMutation(m, property);
    }

    public void addPropertyRemoveToMutation(Mutation m, PropertyRemoveMutation propertyRemove) {
        Text columnQualifier = getPropertyColumnQualifier(propertyRemove);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertyRemove.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        addPropertyRemoveMetadataToMutation(m, propertyRemove);
    }


    public void addPropertyMetadataToMutation(Mutation m, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            Text columnQualifier = getPropertyMetadataColumnQualifier(property, metadataItem.getKey());
            ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
            if (metadataItem.getValue() == null) {
                m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility);
            } else {
                Value metadataValue = new Value(valueSerializer.objectToValue(metadataItem.getValue()));
                m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, metadataValue);
            }
        }
    }

    public void addPropertyRemoveMetadataToMutation(Mutation m, PropertyRemoveMutation propertyRemoveMutation) {
        if (propertyRemoveMutation instanceof PropertyPropertyRemoveMutation) {
            Property property = ((PropertyPropertyRemoveMutation) propertyRemoveMutation).getProperty();
            Metadata metadata = property.getMetadata();
            for (Metadata.Entry metadataItem : metadata.entrySet()) {
                Text columnQualifier = getPropertyMetadataColumnQualifier(property, metadataItem.getKey());
                ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
                m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility);
            }
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem, this.dataDir, rowKey, property);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                StreamUtils.copy(propertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            if (out.hasExceededSizeLimit()) {
                LOGGER.debug(String.format("saved large file to \"%s\" (length: %d)", largeDataStore.getFullHdfsPath(), out.getLength()));
                return new StreamingPropertyValueHdfsRef(largeDataStore.getRelativeFileName(), propertyValue);
            } else {
                return saveStreamingPropertyValueSmall(rowKey, property, out.getSmall(), propertyValue);
            }
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    public void addPropertyRemoveToMutation(Mutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = getPropertyColumnQualifier(property);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            Text metadataEntryColumnQualifier = getPropertyMetadataColumnQualifier(property, metadataEntry.getKey());
            ColumnVisibility metadataEntryVisibility = visibilityToAccumuloVisibility(metadataEntry.getVisibility());
            m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(String rowKey, Property property, byte[] data, StreamingPropertyValue propertyValue) {
        String dataRowKey = createTableDataRowKey(rowKey, property);
        Mutation dataMutation = new Mutation(dataRowKey);
        dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, new Value(data));
        saveDataMutation(dataMutation);
        return new StreamingPropertyValueTableRef(dataRowKey, propertyValue, data);
    }

    protected abstract void saveDataMutation(Mutation dataMutation);

    protected Text getPropertyColumnQualifier(PropertyRemoveMutation propertyRemove) {
        return getValueSeparatedJoined(propertyRemove.getName(), propertyRemove.getKey());
    }

    protected Text getPropertyColumnQualifier(Property property) {
        return getValueSeparatedJoined(property.getName(), property.getKey());
    }

    protected Text getPropertyMetadataColumnQualifier(Property property, String metadataKey) {
        return getValueSeparatedJoined(property.getName(), property.getKey(), property.getVisibility().getVisibilityString(), metadataKey);
    }

    protected static Text getPropertyColumnQualifierWithVisibilityString(Property property) {
        return getValueSeparatedJoined(property.getName(), property.getKey(), property.getVisibility().getVisibilityString());
    }

    protected static Text getValueSeparatedJoined(String... values){
        return new Text(getStringValueSeparatorJoined(values));
    }

    protected static String getStringValueSeparatorJoined(String... values){
        return Joiner.on(VALUE_SEPARATOR).join(values);
    }

    private String createTableDataRowKey(String rowKey, Property property) {
        return getStringValueSeparatorJoined(AccumuloConstants.DATA_ROW_KEY_PREFIX + rowKey, property.getName(), property.getKey());
    }
}
