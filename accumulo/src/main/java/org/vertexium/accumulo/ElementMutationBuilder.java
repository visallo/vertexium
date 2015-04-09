package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.*;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.accumulo.keys.PropertyColumnQualifier;
import org.vertexium.accumulo.keys.PropertyMetadataColumnQualifier;
import org.vertexium.accumulo.serializer.ValueSerializer;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertyPropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.LimitOutputStream;
import org.vertexium.util.Preconditions;
import org.vertexium.util.StreamUtils;

import java.io.IOException;

public abstract class ElementMutationBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementMutationBuilder.class);
    private static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value(new byte[0]);

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
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, visibilityToAccumuloVisibility(vertex.getVisibility()), vertex.getTimestamp(), EMPTY_VALUE);
        for (PropertyDeleteMutation propertyDeleteMutation : vertex.getPropertyDeleteMutations()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : vertex.getPropertySoftDeleteMutations()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
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
        m.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, edge.getTimestamp(), ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), edgeColumnVisibility, edge.getTimestamp(), ElementMutationBuilder.EMPTY_VALUE);
        m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), edgeColumnVisibility, edge.getTimestamp(), ElementMutationBuilder.EMPTY_VALUE);
        for (PropertyDeleteMutation propertyDeleteMutation : edge.getPropertyDeleteMutations()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : edge.getPropertySoftDeleteMutations()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
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

    public boolean alterEdgeVertexOutVertex(Mutation vertexOutMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN));
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
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT));
        vertexInMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addPropertyToMutation(Mutation m, String rowKey, Property property) {
        Text columnQualifier = new PropertyColumnQualifier(property).getColumnQualifier(getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        Value value = new Value(valueSerializer.objectToValue(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, property.getTimestamp(), value);
        addPropertyMetadataToMutation(m, property);
    }

    protected abstract NameSubstitutionStrategy getNameSubstitutionStrategy();

    public void addPropertyDeleteToMutation(Mutation m, PropertyDeleteMutation propertyDelete) {
        Text columnQualifier = new PropertyColumnQualifier(propertyDelete).getColumnQualifier(getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertyDelete.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        addPropertyDeleteMetadataToMutation(m, propertyDelete);
    }

    public void addPropertyMetadataToMutation(Mutation m, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            Text columnQualifier = new PropertyMetadataColumnQualifier(property, metadataItem.getKey()).getColumnQualifier(getNameSubstitutionStrategy());
            ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
            if (metadataItem.getValue() == null) {
                m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility);
            } else {
                Value metadataValue = new Value(valueSerializer.objectToValue(metadataItem.getValue()));
                m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, property.getTimestamp(), metadataValue);
            }
        }
    }

    public void addPropertyDeleteMetadataToMutation(Mutation m, PropertyDeleteMutation propertyDeleteMutation) {
        if (propertyDeleteMutation instanceof PropertyPropertyDeleteMutation) {
            Property property = ((PropertyPropertyDeleteMutation) propertyDeleteMutation).getProperty();
            Metadata metadata = property.getMetadata();
            for (Metadata.Entry metadataItem : metadata.entrySet()) {
                Text columnQualifier = new PropertyMetadataColumnQualifier(property, metadataItem.getKey()).getColumnQualifier(getNameSubstitutionStrategy());
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

    public void addPropertyDeleteToMutation(Mutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = new PropertyColumnQualifier(property).getColumnQualifier(getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            Text metadataEntryColumnQualifier = new PropertyMetadataColumnQualifier(property, metadataEntry.getKey()).getColumnQualifier(getNameSubstitutionStrategy());
            ColumnVisibility metadataEntryVisibility = visibilityToAccumuloVisibility(metadataEntry.getVisibility());
            m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    public void addPropertySoftDeleteToMutation(Mutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        Text columnQualifier = new PropertyColumnQualifier(property).getColumnQualifier(getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, AccumuloElement.SOFT_DELETE_VALUE);
    }

    public void addPropertySoftDeleteToMutation(Mutation m, PropertySoftDeleteMutation propertySoftDelete) {
        Text columnQualifier = new PropertyColumnQualifier(propertySoftDelete).getColumnQualifier(getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertySoftDelete.getVisibility());
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, AccumuloElement.SOFT_DELETE_VALUE);
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
