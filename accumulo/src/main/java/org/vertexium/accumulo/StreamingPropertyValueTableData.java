package org.vertexium.accumulo;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.io.Text;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.ByteRingBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class StreamingPropertyValueTableData extends StreamingPropertyValue {
    private static final long serialVersionUID = 1897402273830254711L;
    public static final Text METADATA_COLUMN_FAMILY = new Text("a"); // this should sort before the data
    public static final Text DATA_COLUMN_FAMILY = new Text("d");
    public static final Text METADATA_LENGTH_COLUMN_QUALIFIER = new Text("length");
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private Long length;
    private final long timestamp;

    public StreamingPropertyValueTableData(
            AccumuloGraph graph,
            String dataRowKey,
            Class valueType,
            Long length,
            long timestamp
    ) {
        super(valueType);
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.length = length;
        this.timestamp = timestamp;
    }

    @Override
    public Long getLength() {
        return length;
    }

    @Override
    public InputStream getInputStream() {
        return new DataTableInputStream();
    }

    private class DataTableInputStream extends InputStream {
        private final ByteRingBuffer buffer = new ByteRingBuffer(1024 * 1024);
        private long timerStartTime;
        private Span trace;
        private ScannerBase scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;
        private long loadedDataLength;

        @Override
        public int read(byte[] dest, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (buffer.getUsed() < len && loadMoreData()) {

            }
            if (buffer.getUsed() == 0) {
                return -1;
            }
            return buffer.read(dest, off, len);
        }

        @Override
        public int read() throws IOException {
            if (buffer.getUsed() < 1) {
                loadMoreData();
                if (buffer.getUsed() == 0) {
                    return -1;
                }
            }
            return buffer.read();
        }

        @Override
        public void close() throws IOException {
            if (scanner != null) {
                scanner.close();
            }
            if (trace != null) {
                trace.stop();
            }

            graph.getGraphLogger().logEndIterator(System.currentTimeMillis() - timerStartTime);
            super.close();
        }

        private boolean loadMoreData() {
            Iterator<Map.Entry<Key, Value>> it = getScannerIterator();
            while (true) {
                if (!it.hasNext()) {
                    return false;
                }
                Map.Entry<Key, Value> column = it.next();
                if (column.getKey().getColumnFamily().equals(METADATA_COLUMN_FAMILY)) {
                    if (column.getKey().getColumnQualifier().equals(METADATA_LENGTH_COLUMN_QUALIFIER)) {
                        length = Longs.fromByteArray(column.getValue().get());
                        continue;
                    }

                    throw new VertexiumException("unexpected metadata column qualifier: " + column.getKey().getColumnQualifier());
                }

                if (column.getKey().getColumnFamily().equals(DATA_COLUMN_FAMILY)) {
                    byte[] data = column.getValue().get();
                    long len = Math.min(data.length, length - loadedDataLength);
                    buffer.write(data, 0, (int) len);
                    loadedDataLength += len;
                    return true;
                }

                throw new VertexiumException("unexpected column family: " + column.getKey().getColumnFamily());
            }
        }

        private Iterator<Map.Entry<Key, Value>> getScannerIterator() {
            if (scannerIterator != null) {
                return scannerIterator;
            }
            scannerIterator = getScanner().iterator();
            return scannerIterator;
        }

        private ScannerBase getScanner() {
            if (scanner != null) {
                return scanner;
            }
            ArrayList<Range> ranges = Lists.newArrayList(RangeUtils.createRangeFromString(dataRowKey));

            timerStartTime = System.currentTimeMillis();
            try {
                scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new org.apache.accumulo.core.security.Authorizations());
            } catch (TableNotFoundException ex) {
                throw new VertexiumException("Could not create scanner", ex);
            }

            IteratorSetting iteratorSetting = new IteratorSetting(
                    80,
                    TimestampFilter.class.getSimpleName(),
                    TimestampFilter.class
            );
            TimestampFilter.setStart(iteratorSetting, timestamp, true);
            TimestampFilter.setEnd(iteratorSetting, timestamp, true);
            scanner.addScanIterator(iteratorSetting);

            graph.getGraphLogger().logStartIterator(scanner);
            trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(1));
            return scanner;
        }
    }
}
