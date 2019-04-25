package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IteratorMapMetadata {
    private final Map<ByteSequence, Map<ByteSequence, Value>> values;

    public IteratorMapMetadata() {
        values = new HashMap<>();
    }

    private IteratorMapMetadata(Map<ByteSequence, Map<ByteSequence, Value>> values) {
        this.values = values;
    }

    public void add(ByteSequence metadataKey, ByteSequence visibility, Value value) {
        Map<ByteSequence, Value> byKey = values.computeIfAbsent(metadataKey, s -> new HashMap<>());
        byKey.put(visibility, value);
    }

    public static IteratorMapMetadata decode(DataInputStream in) throws IOException {
        Map<ByteSequence, Map<ByteSequence, Value>> results = new HashMap<>();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            ByteSequence key = DataInputStreamUtils.decodeByteSequence(in);
            results.put(key, decodeInnerMap(in));
        }
        return new IteratorMapMetadata(results);
    }

    private static Map<ByteSequence, Value> decodeInnerMap(DataInputStream in) throws IOException {
        Map<ByteSequence, Value> results = new HashMap<>();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            ByteSequence key = DataInputStreamUtils.decodeByteSequence(in);
            Value value = DataInputStreamUtils.decodeValue(in);
            results.put(key, value);
        }
        return results;
    }

    public void encode(DataOutputStream out) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<ByteSequence, Map<ByteSequence, Value>> entry1 : values.entrySet()) {
            DataOutputStreamUtils.encodeByteSequence(out, entry1.getKey());
            out.writeInt(entry1.getValue().size());
            for (Map.Entry<ByteSequence, Value> entry2 : entry1.getValue().entrySet()) {
                DataOutputStreamUtils.encodeByteSequence(out, entry2.getKey());
                DataOutputStreamUtils.encodeValue(out, entry2.getValue());
            }
        }
    }

    public Map<ByteSequence, Map<ByteSequence, Value>> getValues() {
        return values;
    }
}
