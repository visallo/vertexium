package org.vertexium.accumulo.util;

import com.google.common.primitives.Longs;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.ElementMutationBuilder;
import org.vertexium.accumulo.StreamingPropertyValueTableDataRef;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.vertexium.accumulo.StreamingPropertyValueTableData.DATA_COLUMN_FAMILY;
import static org.vertexium.accumulo.StreamingPropertyValueTableData.METADATA_COLUMN_FAMILY;
import static org.vertexium.accumulo.StreamingPropertyValueTableData.METADATA_LENGTH_COLUMN_QUALIFIER;

public class DataInDataTableStreamingPropertyValueStorageStrategy implements StreamingPropertyValueStorageStrategy {
    public static final int DEFAULT_PART_SIZE = 10 * 1024;
    private final int dataInDataTablePartSize;

    public DataInDataTableStreamingPropertyValueStorageStrategy(Graph graph, GraphConfiguration configuration) {
        if (!(configuration instanceof AccumuloGraphConfiguration)) {
            throw new VertexiumException("Expected " + AccumuloGraphConfiguration.class.getName() + " found " + configuration.getClass().getName());
        }
        AccumuloGraphConfiguration config = (AccumuloGraphConfiguration) configuration;
        this.dataInDataTablePartSize = config.getInt(AccumuloGraphConfiguration.STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX + ".partSize", DEFAULT_PART_SIZE);
    }

    @Override
    public StreamingPropertyValueRef saveStreamingPropertyValue(
            ElementMutationBuilder elementMutationBuilder,
            String rowKey,
            Property property,
            StreamingPropertyValue streamingPropertyValue
    ) {
        try {
            String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
            InputStream in = streamingPropertyValue.getInputStream();
            byte[] buffer = new byte[dataInDataTablePartSize];
            long offset = 0;
            while (true) {
                int read = in.read(buffer);
                if (read <= 0) {
                    break;
                }
                Mutation dataMutation = new Mutation(dataTableRowKey);
                Text columnQualifier = new Text(String.format("%08x", offset));
                dataMutation.put(DATA_COLUMN_FAMILY, columnQualifier, property.getTimestamp(), new Value(buffer, 0, read));
                elementMutationBuilder.saveDataMutation(dataMutation);
                offset += read;
            }

            Mutation dataMutation = new Mutation(dataTableRowKey);
            dataMutation.put(METADATA_COLUMN_FAMILY, METADATA_LENGTH_COLUMN_QUALIFIER, property.getTimestamp(), new Value(Longs.toByteArray(offset)));
            elementMutationBuilder.saveDataMutation(dataMutation);

            return new StreamingPropertyValueTableDataRef(dataTableRowKey, streamingPropertyValue, offset);
        } catch (Exception ex) {
            throw new VertexiumException("Could not store streaming property value", ex);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        // TODO optimize performance similar to org.vertexium.accumulo.util.OverflowIntoHdfsStreamingPropertyValueStorageStrategy.getInputStreams()
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
    }
}
