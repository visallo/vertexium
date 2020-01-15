package org.vertexium.accumulo.util;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.fs.FileSystem;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.*;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.vertexium.accumulo.ElementMutationBuilder.EMPTY_TEXT;

public class OverflowIntoHdfsStreamingPropertyValueStorageStrategy implements StreamingPropertyValueStorageStrategy {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElementMutationBuilder.class);
    private final FileSystem fileSystem;
    private final long maxStreamingPropertyValueTableDataSize;
    private final String dataDir;
    private final AccumuloGraph graph;

    public OverflowIntoHdfsStreamingPropertyValueStorageStrategy(Graph graph, GraphConfiguration configuration) throws Exception {
        if (!(configuration instanceof AccumuloGraphConfiguration)) {
            throw new VertexiumException("Expected " + AccumuloGraphConfiguration.class.getName() + " found " + configuration.getClass().getName());
        }
        if (!(graph instanceof AccumuloGraph)) {
            throw new VertexiumException("Expected " + AccumuloGraph.class.getName() + " found " + graph.getClass().getName());
        }
        this.graph = (AccumuloGraph) graph;
        AccumuloGraphConfiguration config = (AccumuloGraphConfiguration) configuration;
        this.fileSystem = config.createFileSystem();
        this.maxStreamingPropertyValueTableDataSize = config.getMaxStreamingPropertyValueTableDataSize();
        this.dataDir = config.getDataDir();
    }

    @Override
    public StreamingPropertyValueRef saveStreamingPropertyValue(
        ElementMutationBuilder elementMutationBuilder,
        String rowKey,
        Property property,
        StreamingPropertyValue streamingPropertyValue
    ) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem, this.dataDir, rowKey, property);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                IOUtils.copy(streamingPropertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            if (out.hasExceededSizeLimit()) {
                LOGGER.debug("saved large file to \"%s\" (length: %d)", largeDataStore.getFullHdfsPath(), out.getLength());
                return new StreamingPropertyValueHdfsRef(largeDataStore.getRelativeFileName(), streamingPropertyValue);
            } else {
                return saveStreamingPropertyValueSmall(elementMutationBuilder, rowKey, property, out.getSmall(), streamingPropertyValue);
            }
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.fileSystem.close();
        } catch (IOException ex) {
            throw new VertexiumException("Could not close filesystem", ex);
        }
    }

    @Override
    public List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        List<StreamingPropertyValueTable> notLoadedTableSpvs = streamingPropertyValues.stream()
            .filter((spv) -> spv instanceof StreamingPropertyValueTable)
            .map((spv) -> (StreamingPropertyValueTable) spv)
            .filter((spv) -> !spv.isDataLoaded())
            .collect(Collectors.toList());

        List<String> dataRowKeys = notLoadedTableSpvs.stream()
            .map(StreamingPropertyValueTable::getDataRowKey)
            .collect(Collectors.toList());

        Map<String, byte[]> tableInputStreams = streamingPropertyValueTableDatas(dataRowKeys);
        notLoadedTableSpvs
            .forEach((spv) -> {
                String dataRowKey = spv.getDataRowKey();
                byte[] bytes = tableInputStreams.get(dataRowKey);
                if (bytes == null) {
                    throw new VertexiumException("Could not find StreamingPropertyValue data: " + dataRowKey);
                }
                spv.setData(bytes);
            });

        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }

    private Map<String, byte[]> streamingPropertyValueTableDatas(List<String> dataRowKeys) {
        try {
            if (dataRowKeys.size() == 0) {
                return Collections.emptyMap();
            }

            List<org.apache.accumulo.core.data.Range> ranges = dataRowKeys.stream()
                .map(RangeUtils::createRangeFromString)
                .collect(Collectors.toList());

            final long timerStartTime = System.currentTimeMillis();
            ScannerBase scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new org.apache.accumulo.core.security.Authorizations());

            graph.getGraphLogger().logStartIterator(graph.getDataTableName(), scanner);
            Span trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(dataRowKeys.size()));
            try {
                Map<String, byte[]> results = new HashMap<>();
                for (Map.Entry<Key, Value> col : scanner) {
                    results.put(col.getKey().getRow().toString(), col.getValue().get());
                }
                return results;
            } finally {
                scanner.close();
                trace.stop();
                graph.getGraphLogger().logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(
        ElementMutationBuilder elementMutationBuilder,
        String rowKey,
        Property property,
        byte[] data,
        StreamingPropertyValue propertyValue
    ) {
        String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
        CompletableMutation dataMutation = new CompletableMutation(dataTableRowKey);
        dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, property.getTimestamp(), new Value(data));
        elementMutationBuilder.saveDataMutation(dataMutation);
        return new StreamingPropertyValueTableRef(dataTableRowKey, propertyValue, data);
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getDataDir() {
        return dataDir;
    }
}
