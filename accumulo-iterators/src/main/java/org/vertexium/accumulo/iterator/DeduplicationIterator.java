package org.vertexium.accumulo.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * An iterator for iterating over unique consecutive values for a given key.
 *
 * This iterator should be used only for major compaction, on a table where blind writing is enabled.
 * It will ensure that the history of the records is preserved, but will remove records that are
 * consecutive and identical (same key and value but different timestamp). If two records are
 * identical (have the same key and value, except the timestamp), but are not consecutive, those
 * records will be left alone. This ensures the preservation of record history while removing
 * duplicate data.
 */

public class DeduplicationIterator extends WrappingIterator {

    private Key currentKey = new Key();
    private Value currentValue = new Value();
    private Key prevKey = new Key();
    private Value prevValue = new Value();
    private int numVersions;

    private Range range;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;

    @Override
    public DeduplicationIterator deepCopy(IteratorEnvironment env) {
        DeduplicationIterator copy = new DeduplicationIterator();
        copy.setSource(getSource().deepCopy(env));
        return copy;
    }

    @Override
    public void next() throws IOException {
        if (getSource().hasTop()) {
            if (getSource().getTopKey().equals(currentKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                numVersions++;
            } else {
                resetVersionCount();
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException {
        // do not want to seek to the middle of a row
        Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
        this.range = seekRange;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        super.seek(seekRange, columnFamilies, inclusive);
        resetVersionCount();

        if (range.getStartKey() != null) {
            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }

    private void resetVersionCount() {
        if (super.hasTop()) {
            currentKey.set(getSource().getTopKey());
            currentValue.set(getSource().getTopValue().get());
            prevKey.set(getSource().getTopKey());
            prevValue.set(getSource().getTopValue().get());
        }
        numVersions = 1;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                     IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        this.numVersions = 0;

    }

    @Override
    public Key getTopKey() {
        while (super.hasTop()) {
            Key k = super.getTopKey();
            Value v = super.getTopValue();
            if (k.equals(currentKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                prevKey.set(currentKey);
                currentKey.set(k);
                if (v.equals(currentValue)) {
                    prevValue.set(currentValue.get());
                } else {
                    currentValue.set(v.get());
                    break;
                }
                try {
                    super.next();
                } catch (IOException e) {
                    throw new RuntimeException("Exception calling super.next(): " + e.getMessage());
                }
            } else {
                prevKey.set(currentKey);
                prevValue.set(currentValue.get());
                break;
            }
        }
        if (!super.hasTop() && prevValue.equals(currentValue)) {
            return currentKey;
        }
        return prevKey;
    }

    public Value getTopValue() {
        return prevValue;
    }

}
