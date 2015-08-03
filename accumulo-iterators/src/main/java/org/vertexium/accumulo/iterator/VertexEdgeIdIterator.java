package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

public class VertexEdgeIdIterator extends RowEncodingIterator {
    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
        throw new VertexiumAccumuloIteratorException("Not Implemented");
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (Key key : keys) {
            if (!key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE)
                    && !key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE)) {
                continue;
            }
            Text edgeId = key.getColumnQualifier();
            DataOutputStreamUtils.encodeByteArray(out, edgeId.getBytes());
        }
        return new Value(baos.toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new VertexEdgeIdIterator();
    }

    public static Iterable<ByteArrayWrapper> decodeValue(final Value value) {
        return new Iterable<ByteArrayWrapper>() {
            @Override
            public Iterator<ByteArrayWrapper> iterator() {
                ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
                final DataInputStream in = new DataInputStream(bais);
                return new Iterator<ByteArrayWrapper>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return in.available() > 0;
                        } catch (IOException e) {
                            throw new VertexiumAccumuloIteratorException("Could not get available", e);
                        }
                    }

                    @Override
                    public ByteArrayWrapper next() {
                        try {
                            return DataInputStreamUtils.decodeByteArrayWrapper(in);
                        } catch (IOException e) {
                            throw new VertexiumAccumuloIteratorException("Could not read text", e);
                        }
                    }

                    @Override
                    public void remove() {
                        throw new VertexiumAccumuloIteratorException("not implemented");
                    }
                };
            }
        };
    }
}
