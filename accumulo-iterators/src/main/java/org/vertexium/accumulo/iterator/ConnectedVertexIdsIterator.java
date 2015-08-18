package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.*;
import java.util.*;

public class ConnectedVertexIdsIterator extends RowEncodingIterator {
    public static Set<String> decodeValue(Value value) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
        DataInputStream in = new DataInputStream(bais);
        return DataInputStreamUtils.decodeSetOfStrings(in);
    }

    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
        throw new VertexiumAccumuloIteratorException("not implemented");
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
        Map<Text, String> inVertexIds = new HashMap<>();
        Map<Text, String> outVertexIds = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            Value value = values.get(i);
            if (key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE)) {
                EdgeInfo edgeInfo = new EdgeInfo(value.get(), key.getTimestamp());
                outVertexIds.put(key.getColumnQualifier(), edgeInfo.getVertexId());
            } else if (key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE_HIDDEN)) {
                outVertexIds.remove(key.getColumnQualifier());
            } else if (key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE)) {
                EdgeInfo edgeInfo = new EdgeInfo(value.get(), key.getTimestamp());
                inVertexIds.put(key.getColumnQualifier(), edgeInfo.getVertexId());
            } else if (key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE_HIDDEN)) {
                inVertexIds.remove(key.getColumnQualifier());
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        Set<String> vertexIds = new HashSet<>();
        vertexIds.addAll(inVertexIds.values());
        vertexIds.addAll(outVertexIds.values());
        DataOutputStreamUtils.encodeSetOfStrings(out, vertexIds);
        return new Value(baos.toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new ConnectedVertexIdsIterator();
    }
}
