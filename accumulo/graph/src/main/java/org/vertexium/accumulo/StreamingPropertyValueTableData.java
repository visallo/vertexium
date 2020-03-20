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
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.io.Text;
import org.vertexium.StreamingPropertyValueChunk;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.iterator.RowTimestampFilter;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.ByteRingBuffer;
import org.vertexium.util.ClosingIterator;
import org.vertexium.util.DelegatingStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

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

    public static Stream<StreamingPropertyValueChunk> readChunks(AccumuloGraph graph, Set<StreamingPropertyValueTableData> spvs) {
        if (spvs.size() == 0) {
            return Stream.empty();
        }

        try {
            List<Range> ranges = new ArrayList<>();
            Map<String, Set<StreamingPropertyValueTableData>> streamingPropertyValuesByRowKey = new HashMap<>();
            Map<Text, RowTimestampFilter.Timestamp> timestamps = new HashMap<>();
            for (StreamingPropertyValueTableData spv : spvs) {
                ranges.add(RangeUtils.createRangeFromString(spv.dataRowKey));
                Set<StreamingPropertyValueTableData> list = streamingPropertyValuesByRowKey.computeIfAbsent(
                    spv.dataRowKey,
                    s -> new HashSet<>()
                );
                list.add(spv);
                timestamps.put(
                    new Text(spv.dataRowKey),
                    new RowTimestampFilter.Timestamp(spv.timestamp, true, spv.timestamp, true)
                );
            }

            ScannerBase scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new Authorizations());

            IteratorSetting iteratorSetting = new IteratorSetting(
                80,
                RowTimestampFilter.class.getSimpleName(),
                RowTimestampFilter.class
            );
            RowTimestampFilter.setTimestamps(iteratorSetting, timestamps);
            scanner.addScanIterator(iteratorSetting);

            Iterator<Map.Entry<Key, Value>> scannerIterator = scanner.iterator();

            Map<String, Long> streamingPropertyValueLengthsByRowKey = new HashMap<>();
            Map<String, Long> streamingPropertyValueReadLengthsByRowKey = new HashMap<>();
            AtomicBoolean closeCalled = new AtomicBoolean();
            Runnable handleClose = () -> {
                if (closeCalled.get()) {
                    return;
                }
                closeCalled.set(true);
                if (!scannerIterator.hasNext()) {
                    for (Map.Entry<String, Long> entry : streamingPropertyValueLengthsByRowKey.entrySet()) {
                        String rowKey = entry.getKey();
                        Long expectedLength = entry.getValue();
                        Long readLength = streamingPropertyValueReadLengthsByRowKey.get(entry.getKey());
                        if (expectedLength == 0 && readLength == null) {
                            // no data found which is OK
                        } else if (readLength == null || !readLength.equals(expectedLength)) {
                            throw new VertexiumException(String.format("Expected streaming property value length of %d only read %d (rowKey: %s)", expectedLength, readLength, rowKey));
                        }
                    }
                }
                scanner.close();
            };

            Stream<StreamingPropertyValueChunk> results = stream(new ClosingIterator<>(scannerIterator, handleClose))
                .flatMap(column -> {
                    String rowKey = column.getKey().getRow().toString();
                    Set<StreamingPropertyValueTableData> streamingPropertyValues = streamingPropertyValuesByRowKey.get(rowKey);
                    if (streamingPropertyValues == null) {
                        throw new VertexiumException(String.format("Found row with key %s but was not in ranges", rowKey));
                    }

                    if (column.getKey().getColumnFamily().equals(METADATA_COLUMN_FAMILY)) {
                        if (column.getKey().getColumnQualifier().equals(METADATA_LENGTH_COLUMN_QUALIFIER)) {
                            long length = Longs.fromByteArray(column.getValue().get());
                            streamingPropertyValueLengthsByRowKey.put(rowKey, length);
                            return Stream.empty();
                        }

                        throw new VertexiumException(String.format("unexpected metadata column qualifier: %s (row: %s)", column.getKey().getColumnQualifier(), column.getKey().getRow()));
                    }

                    if (column.getKey().getColumnFamily().equals(DATA_COLUMN_FAMILY)) {
                        Long totalLength = streamingPropertyValueLengthsByRowKey.get(rowKey);
                        if (totalLength == null) {
                            throw new VertexiumException("unexpected missing length (row: " + column.getKey().getRow() + ")");
                        }
                        long readLength = streamingPropertyValueReadLengthsByRowKey.getOrDefault(rowKey, 0L);

                        byte[] data = column.getValue().get();
                        int chunkSize = data.length;
                        readLength += chunkSize;
                        if (readLength > totalLength) {
                            throw new VertexiumException(String.format("too many bytes read. Expected %d found %d (row: %s)", totalLength, readLength, column.getKey().getRow()));
                        }
                        boolean isLast = readLength == totalLength;
                        streamingPropertyValueReadLengthsByRowKey.put(rowKey, readLength);
                        return streamingPropertyValues.stream()
                            .map(spv -> new StreamingPropertyValueChunk(spv, data, chunkSize, isLast));
                    }

                    throw new VertexiumException(String.format("unexpected column family: %s (row: %s)", column.getKey().getColumnFamily(), column.getKey().getRow()));
                })
                .filter(Objects::nonNull);
            return new DelegatingStream<>(results)
                .onClose(handleClose);
        } catch (TableNotFoundException ex) {
            throw new VertexiumException("Failed to read chunks", ex);
        }
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
        private long previousLoadedDataLength;
        private long loadedDataLength;
        private boolean closed;

        private long markRowIndex = 0;
        private long markByteOffsetInRow = 0;
        private long markLoadedDataLength = 0;
        private long currentDataRowIndex = -1;
        private long currentByteOffsetInRow;

        @Override
        public int read(byte[] dest, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            len = Math.min(len, buffer.getSize());
            while (buffer.getUsed() == 0 && loadMoreData()) {

            }
            if (buffer.getUsed() == 0) {
                return -1;
            }

            int bytesRead = buffer.read(dest, off, len);
            currentByteOffsetInRow += bytesRead;
            return bytesRead;
        }

        @Override
        public int read() throws IOException {
            if (buffer.getUsed() < 1) {
                loadMoreData();
                if (buffer.getUsed() == 0) {
                    return -1;
                }
            }
            currentByteOffsetInRow++;
            return buffer.read();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            scannerIterator = null;
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
            if (trace != null) {
                trace.stop();
                trace = null;
            }

            graph.getGraphLogger().logEndIterator(System.currentTimeMillis() - timerStartTime);
            super.close();
            closed = true;
        }

        private boolean loadMoreData() throws IOException {
            if (closed) {
                return false;
            }
            Iterator<Map.Entry<Key, Value>> it = getScannerIterator();
            while (true) {
                if (!it.hasNext()) {
                    close();
                    return false;
                }
                Map.Entry<Key, Value> column = it.next();
                if (column.getKey().getColumnFamily().equals(METADATA_COLUMN_FAMILY)) {
                    if (column.getKey().getColumnQualifier().equals(METADATA_LENGTH_COLUMN_QUALIFIER)) {
                        length = Longs.fromByteArray(column.getValue().get());
                        continue;
                    }

                    throw new VertexiumException("unexpected metadata column qualifier: " + column.getKey().getColumnQualifier() + " (row: " + column.getKey().getRow() + ")");
                }

                if (column.getKey().getColumnFamily().equals(DATA_COLUMN_FAMILY)) {
                    currentDataRowIndex++;
                    currentByteOffsetInRow = 0;

                    byte[] data = column.getValue().get();
                    if (length == null) {
                        throw new VertexiumException("unexpected missing length (row: " + column.getKey().getRow() + ")");
                    }
                    long len = Math.min(data.length, length - loadedDataLength);
                    buffer.write(data, 0, (int) len);
                    previousLoadedDataLength = loadedDataLength;
                    loadedDataLength += len;
                    return true;
                }

                throw new VertexiumException("unexpected column family: " + column.getKey().getColumnFamily() + " (row: " + column.getKey().getRow() + ")");
            }
        }

        private Iterator<Map.Entry<Key, Value>> getScannerIterator() throws IOException {
            if (closed) {
                throw new IOException("stream already closed");
            }
            if (scannerIterator != null) {
                return scannerIterator;
            }
            scannerIterator = getScanner().iterator();
            return scannerIterator;
        }

        private ScannerBase getScanner() throws IOException {
            if (closed) {
                throw new IOException("stream already closed");
            }
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

            graph.getGraphLogger().logStartIterator(graph.getDataTableName(), scanner);
            trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(1));
            return scanner;
        }

        @Override
        public synchronized void mark(int readlimit) {
            markRowIndex = Math.max(0, currentDataRowIndex);
            markByteOffsetInRow = currentByteOffsetInRow;
            markLoadedDataLength = previousLoadedDataLength;
        }

        @Override
        public synchronized void reset() throws IOException {
            buffer.clear();
            if (scannerIterator != null) {
                scannerIterator = null;
            }

            closed = false;

            currentDataRowIndex = -1;
            currentByteOffsetInRow = 0;
            loadedDataLength = markLoadedDataLength;

            Iterator<Map.Entry<Key, Value>> it = getScannerIterator();
            while (true) {
                if (!it.hasNext()) {
                    close();
                    return;
                }
                Map.Entry<Key, Value> column = it.next();
                if (column.getKey().getColumnFamily().equals(DATA_COLUMN_FAMILY)) {
                    currentDataRowIndex++;
                    currentByteOffsetInRow = 0;
                    if (currentDataRowIndex == markRowIndex) {
                        byte[] data = column.getValue().get();
                        long len = Math.min(data.length, length - loadedDataLength);
                        buffer.write(data, 0, (int) len);
                        loadedDataLength += len;
                        while (currentByteOffsetInRow != markByteOffsetInRow) {
                            buffer.read();
                            currentByteOffsetInRow++;
                        }
                        return;
                    }
                }
            }
        }

        @Override
        public boolean markSupported() {
            return true;
        }
    }
}
