package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.collections.BufferOverflowException;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class RowDeduplicationIterator
        implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

    public static final String MAX_BUFFER_SIZE_OPT = "maxBufferSize";
    private static final long DEFAULT_MAX_BUFFER_SIZE = Long.MAX_VALUE;
    private static final List<String> COLUMN_FAMILY_TYPES = Arrays.asList("PROP", "PROPD");
    protected SortedKeyValueIterator<Key, Value> sourceIter;
    private Key topKey = null;
    private Value topValue = null;
    private long maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        RowDeduplicationIterator newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.sourceIter = sourceIter.deepCopy(env);
        newInstance.maxBufferSize = maxBufferSize;
        return newInstance;
    }

    List<KeyValue> sortedRow = new ArrayList<>();
    Map<Key, Value> unsortedRow = new LinkedHashMap<>();

    private void prepKeys() throws IOException {
        long kvBufSize = 0;
        if (topKey != null) {
            return;
        }
        Text currentRow;
        do {
            if (!sourceIter.hasTop()) {
                return;
            }
            currentRow = new Text(sourceIter.getTopKey().getRow());
            unsortedRow.clear();
            sortedRow.clear();
            while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow)) {
                Key sourceTopKey = sourceIter.getTopKey();
                Value sourceTopValue = sourceIter.getTopValue();
                KeyValue keyVal = new KeyValue();
                keyVal.set(new Key(sourceTopKey), new Value((sourceTopValue)));
                sortedRow.add(keyVal);
                unsortedRow.put(new Key(sourceTopKey), new Value(sourceTopValue));
                kvBufSize += sourceTopKey.getSize() + sourceTopValue.getSize() + 128;
                if (kvBufSize > maxBufferSize) {
                    throw new BufferOverflowException(
                            "Exceeded buffer size of " + maxBufferSize + " for row: " + sourceTopKey.getRow());
                }
                sourceIter.next();
            }
        } while (!filter(currentRow));

        //sort the list timestamp but also preserve original layout
        sortedRow.sort(Comparator.comparing(KeyValue::getTimestamp));
        //eliminate duplicates from sorted list and adjust unsorted
        //instead of the top key, emit the next non duplicate key
        deduplicateRow();
        if (unsortedRow.keySet().iterator().hasNext()) {
            topKey = new Key(unsortedRow.keySet().iterator().next());
            topValue = new Value(unsortedRow.remove(unsortedRow.keySet().iterator().next()));
        } else {
            topKey = new Key(unsortedRow.keySet().iterator().next());
            topValue = new Value(unsortedRow.remove(unsortedRow.keySet().iterator().next()));
        }

    }

    private void deduplicateRow() {
        Key dedupeKey = new Key();
        Value dedupeValue = new Value();
        Key lastRemovedPropKey = null;
        Key prevKey = new Key();
        for (KeyValue kv : sortedRow) {
            Key k = kv.peekKey();
            Value v = kv.peekValue();
            if (k.equals(dedupeKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS) &&
                    COLUMN_FAMILY_TYPES.contains(k.getColumnFamily().toString())) {
                if (v.equals(dedupeValue)) {
                    //since value is equal, remove duplicate PROP from unsorted
                    if (k.getColumnFamily().toString().equalsIgnoreCase("PROP")) {
                        lastRemovedPropKey = k;
                        unsortedRow.remove(k);
                    } else if (prevKey.equals(k, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                        unsortedRow.remove(k);

                    }
                } else {
                    dedupeValue = v;
                }
            } else {
                if (COLUMN_FAMILY_TYPES.contains(k.getColumnFamily().toString())) {
                    // set only for PROP and PROPD
                    dedupeKey = k;
                    dedupeValue = v;
                } else if (k.getColumnFamily().toString().equalsIgnoreCase("PROPMETA") &&
                        lastRemovedPropKey != null &&
                        k.getTimestamp() == lastRemovedPropKey.getTimestamp()) {
                    // Remove matching PROPMETA
                    unsortedRow.remove(k);
                }
            }
            prevKey = k;
        }

    }

    /**
     * @param currentRow All keys have this in their row portion (do not modify!).
     * @return true if we want to keep the row, false if we want to skip it
     */
    protected boolean filter(Text currentRow) {
        return true;
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        sourceIter = source;
        if (options.containsKey(MAX_BUFFER_SIZE_OPT)) {
            maxBufferSize = AccumuloConfiguration.getMemoryInBytes(
                    options.get(MAX_BUFFER_SIZE_OPT));
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        String desc = "This iterator encapsulates an entire row of Key/Value pairs"
                + " into a single Key/Value pair.";
        String bufferDesc = "Maximum buffer size (in accumulo memory spec) to use"
                + " for buffering keys before throwing a BufferOverflowException.";
        HashMap<String, String> namedOptions = new HashMap<>();
        namedOptions.put(MAX_BUFFER_SIZE_OPT, bufferDesc);
        return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        String maxBufferSizeStr = options.get(MAX_BUFFER_SIZE_OPT);
        try {
            AccumuloConfiguration.getMemoryInBytes(maxBufferSizeStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to parse opt " + MAX_BUFFER_SIZE_OPT + " " + maxBufferSizeStr, e);
        }
        return true;
    }

    @Override
    public void next() throws IOException {
        if (unsortedRow.keySet().iterator().hasNext()) {
            topKey = new Key(unsortedRow.keySet().iterator().next());
            topValue = new Value(unsortedRow.remove(unsortedRow.keySet().iterator().next()));
        } else {
            topKey = null;
            topValue = null;
            prepKeys();
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException {
        topKey = null;
        topValue = null;

        Key sk = range.getStartKey();

        if (sk != null && sk.getColumnFamilyData().length() == 0
                && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
                && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
            // assuming that we are seeking using a key previously returned by this iterator
            // therefore go to the next row
            Key followingRowKey = sk.followingKey(PartialKey.ROW);
            if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0) {
                return;
            }

            range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(),
                    range.isEndKeyInclusive());
        }

        sourceIter.seek(range, columnFamilies, inclusive);
        prepKeys();
    }

}
