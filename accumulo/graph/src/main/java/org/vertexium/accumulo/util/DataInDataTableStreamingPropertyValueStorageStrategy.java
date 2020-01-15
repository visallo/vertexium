package org.vertexium.accumulo.util;

import com.google.common.primitives.Longs;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.*;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.accumulo.StreamingPropertyValueTableData.*;

public class DataInDataTableStreamingPropertyValueStorageStrategy implements StreamingPropertyValueStorageStrategy {
    public static final int DEFAULT_PART_SIZE = 10 * 1024;
    private final int dataInDataTablePartSize;
    private final AccumuloGraph graph;

    public DataInDataTableStreamingPropertyValueStorageStrategy(Graph graph, GraphConfiguration configuration) {
        if (!(graph instanceof AccumuloGraph)) {
            throw new VertexiumException("Expected " + AccumuloGraph.class.getName() + " found " + graph.getClass().getName());
        }
        this.graph = (AccumuloGraph) graph;

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
        try (InputStream in = streamingPropertyValue.getInputStream()) {
            String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
            byte[] buffer = new byte[dataInDataTablePartSize];
            long offset = 0;
            while (true) {
                int read = in.read(buffer);
                if (read <= 0) {
                    break;
                }
                CompletableMutation dataMutation = new CompletableMutation(dataTableRowKey);
                Text columnQualifier = new Text(String.format("%08x", offset));
                dataMutation.put(DATA_COLUMN_FAMILY, columnQualifier, property.getTimestamp(), new Value(buffer, 0, read));
                elementMutationBuilder.saveDataMutation(dataMutation);
                offset += read;
            }

            if (streamingPropertyValue.getLength() != null && offset != streamingPropertyValue.getLength()) {
                throw new VertexiumException(String.format(
                    "streaming property value reports it's size as %d but only read %d from the input stream",
                    streamingPropertyValue.getLength(),
                    offset
                ));
            }

            CompletableMutation dataMutation = new CompletableMutation(dataTableRowKey);
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

    @Override
    public Stream<StreamingPropertyValueChunk> readStreamingPropertyValueChunks(Iterable<StreamingPropertyValue> streamingPropertyValues) {
        Set<StreamingPropertyValueTableData> streamingPropertyValueTableData = new HashSet<>();
        List<StreamingPropertyValue> otherStreamingPropertyValue = new ArrayList<>();
        for (StreamingPropertyValue spv : streamingPropertyValues) {
            if (spv instanceof StreamingPropertyValueTableData) {
                streamingPropertyValueTableData.add((StreamingPropertyValueTableData) spv);
            } else {
                otherStreamingPropertyValue.add(spv);
            }
        }

        Stream<StreamingPropertyValueChunk> results = StreamingPropertyValueTableData.readChunks(graph, streamingPropertyValueTableData);
        if (otherStreamingPropertyValue.size() > 0) {
            results = Stream.concat(results, StreamingPropertyValue.readChunks(otherStreamingPropertyValue));
        }
        return results;
    }
}
