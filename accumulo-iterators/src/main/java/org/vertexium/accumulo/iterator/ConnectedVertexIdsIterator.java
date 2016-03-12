package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
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
    public static final String SETTING_LABEL_PREFIX = "label:";
    private Set<String> labels;

    public static void setLabels(IteratorSetting settings, String[] labels) {
        if (labels == null) {
            return;
        }
        for (int i = 0; i < labels.length; i++) {
            settings.addOption(SETTING_LABEL_PREFIX + i, labels[i]);
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        Set<String> labels = new HashSet<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            if (option.getKey().startsWith(SETTING_LABEL_PREFIX)) {
                labels.add(option.getValue());
            }
        }
        this.labels = labels.size() == 0 ? null : labels;
    }

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
                if (isMatch(edgeInfo)) {
                    outVertexIds.put(key.getColumnQualifier(), edgeInfo.getVertexId());
                }
            } else if (key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE_HIDDEN)
                    || key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE_SOFT_DELETE)) {
                outVertexIds.remove(key.getColumnQualifier());
            } else if (key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE)) {
                EdgeInfo edgeInfo = new EdgeInfo(value.get(), key.getTimestamp());
                if (isMatch(edgeInfo)) {
                    inVertexIds.put(key.getColumnQualifier(), edgeInfo.getVertexId());
                }
            } else if (key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE_HIDDEN)
                    || key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE_SOFT_DELETE)) {
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

    private boolean isMatch(EdgeInfo edgeInfo) {
        return labels == null || labels.contains(edgeInfo.getLabel());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new ConnectedVertexIdsIterator();
    }
}
