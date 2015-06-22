package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class CountingIterator implements SortedKeyValueIterator<Key, Value> {
    private SortedKeyValueIterator<Key, Value> source;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }

    @Override
    public boolean hasTop() {
        return source.hasTop();
    }

    @Override
    public void next() throws IOException {

    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return source.getTopKey();
    }

    @Override
    public Value getTopValue() {
        try {
            long count = 0;
            while (source.hasTop()) {
                count++;
                source.next();
            }
            return new Value(LongCombiner.FIXED_LEN_ENCODER.encode(count));
        } catch (IOException e) {
            throw new RuntimeException("could not iterate", e);
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        throw new UnsupportedOperationException();
    }
}
