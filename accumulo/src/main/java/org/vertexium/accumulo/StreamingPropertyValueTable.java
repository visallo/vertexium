package org.vertexium.accumulo;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.vertexium.accumulo.AccumuloGraph.GRAPH_LOGGER;

public class StreamingPropertyValueTable extends StreamingPropertyValue {
    private static final long serialVersionUID = 400244414843534240L;
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private final long timestamp;
    private transient byte[] data;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, StreamingPropertyValueTableRef valueRef, long timestamp) {
        super(valueRef.getValueType());
        this.timestamp = timestamp;
        this.searchIndex(valueRef.isSearchIndex());
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.data = valueRef.getData();
    }

    @Override
    public Long getLength() {
        ensureDataLoaded();
        return (long) this.data.length;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    public boolean isDataLoaded() {
        return this.data != null;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        // we need to store the data here to handle the case that the mutation hasn't been flushed yet but the element is
        // passed to the search indexer to be indexed and we can't get the value yet.
        ensureDataLoaded();
        return new ByteArrayInputStream(this.data);
    }

    private void ensureDataLoaded() {
        if (!isDataLoaded()) {
            this.data = streamingPropertyValueTableData(this.dataRowKey, this.timestamp);
        }
    }

    public byte[] streamingPropertyValueTableData(String dataRowKey, Long timestamp) {
        try {
            List<Range> ranges = Lists.newArrayList(RangeUtils.createRangeFromString(dataRowKey));

            long timerStartTime = System.currentTimeMillis();
            ScannerBase scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new org.apache.accumulo.core.security.Authorizations());
            if (timestamp != null && !DataTableRowKey.isLegacy(dataRowKey)) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        80,
                        TimestampFilter.class.getSimpleName(),
                        TimestampFilter.class
                );
                TimestampFilter.setStart(iteratorSetting, timestamp, true);
                TimestampFilter.setEnd(iteratorSetting, timestamp, true);
                scanner.addScanIterator(iteratorSetting);
            }

            GRAPH_LOGGER.logStartIterator(scanner);
            Span trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(1));
            try {
                byte[] result = null;
                for (Map.Entry<Key, Value> col : scanner) {
                    String foundKey = col.getKey().getRow().toString();
                    byte[] value = col.getValue().get();
                    if (foundKey.equals(dataRowKey)) {
                        result = value;
                    }
                }
                if (result == null) {
                    throw new VertexiumException("Could not find data with key: " + dataRowKey);
                }
                return result;
            } finally {
                scanner.close();
                trace.stop();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
    }
}
