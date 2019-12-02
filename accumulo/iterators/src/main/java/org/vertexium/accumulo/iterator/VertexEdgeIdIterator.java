package org.vertexium.accumulo.iterator;

import com.google.protobuf.ByteArrayByteString;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;
import org.vertexium.accumulo.iterator.model.proto.EdgeIds;

import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

public class VertexEdgeIdIterator extends RowEncodingIterator {
    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) {
        throw new VertexiumAccumuloIteratorException("Not Implemented");
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) {
        EdgeIds.Builder edgeIds = EdgeIds.newBuilder();
        for (Key key : keys) {
            if (!key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE)
                && !key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE)) {
                continue;
            }
            Text edgeId = key.getColumnQualifier();
            edgeIds.addEdgeIds(new ByteArrayByteString(edgeId.getBytes()));
        }
        return new Value(edgeIds.build().toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new VertexEdgeIdIterator();
    }

    public static Collection<String> decodeValue(Value value) {
        try {
            EdgeIds edgeIds = EdgeIds.parseFrom(value.get());
            return edgeIds.getEdgeIdsList().stream()
                .map(ByteString::toStringUtf8)
                .collect(Collectors.toList());
        } catch (InvalidProtocolBufferException ex) {
            throw new VertexiumAccumuloIteratorException("Failed to decode edge ids", ex);
        }
    }
}
