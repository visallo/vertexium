package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class DataOutputStreamUtils {
    public static final Charset CHARSET = Charset.forName("utf8");
    public static final byte EDGE_LABEL_ONLY_MARKER = 1;
    public static final byte EDGE_LABEL_WITH_REFS_MARKER = 2;

    public static void encodeTextList(DataOutputStream out, Collection<Text> texts) throws IOException {
        if (texts == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(texts.size());
        for (Text text : texts) {
            encodeText(out, text);
        }
    }

    public static void encodeByteSequenceList(DataOutputStream out, Collection<ByteSequence> byteSequences) throws IOException {
        if (byteSequences == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(byteSequences.size());
        for (ByteSequence byteSequence : byteSequences) {
            encodeByteSequence(out, byteSequence);
        }
    }

    public static void encodeStringSet(DataOutputStream out, Set<String> set) throws IOException {
        if (set == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(set.size());
        for (String item : set) {
            encodeString(out, item);
        }
    }

    public static void encodeText(DataOutputStream out, Text text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(text.getLength());
        out.write(text.getBytes(), 0, text.getLength());
    }

    public static void encodeByteSequence(DataOutputStream out, ByteSequence byteSequence) throws IOException {
        if (byteSequence == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(byteSequence.length());
        out.write(byteSequence.getBackingArray(), byteSequence.offset(), byteSequence.length());
    }

    public static void encodeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void encodeValue(DataOutputStream out, Value value) throws IOException {
        encodeByteArray(out, value == null ? null : value.get());
    }

    public static void encodeIntArray(DataOutputStream out, Collection<Integer> integers) throws IOException {
        if (integers == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(integers.size());
        for (Integer i : integers) {
            out.writeInt(i);
        }
    }

    public static void encodeString(DataOutputStream out, String text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = text.getBytes(CHARSET);
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    public static void encodeLong(DataOutputStream out, Long value) throws IOException {
        if (value == null) {
            out.writeByte(0x00);
            return;
        }
        out.writeByte(0x01);
        out.writeLong(value);
    }

    public static void encodePropertyMetadataEntry(DataOutputStream out, List<IteratorMetadataEntry> metadataEntries) throws IOException {
        if (metadataEntries == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(metadataEntries.size());
        for (IteratorMetadataEntry metadataEntry : metadataEntries) {
            encodeByteSequence(out, metadataEntry.metadataKey);
            encodeByteSequence(out, metadataEntry.metadataVisibility);
            out.writeInt(metadataEntry.value.length);
            out.write(metadataEntry.value);
        }
    }

    public static void encodeEdges(DataOutputStream out, EdgesWithEdgeInfo edges, boolean edgeLabelsOnly) throws IOException {
        out.write(edgeLabelsOnly ? EDGE_LABEL_ONLY_MARKER : EDGE_LABEL_WITH_REFS_MARKER);

        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = getEdgesByLabel(edges);
        out.writeInt(edgesByLabels.size());
        for (Map.Entry<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> entry : edgesByLabels.entrySet()) {
            encodeByteArray(out, entry.getKey().getData());
            out.writeInt(entry.getValue().size());
            if (!edgeLabelsOnly) {
                for (Map.Entry<Text, EdgeInfo> edgeEntry : entry.getValue()) {
                    encodeText(out, edgeEntry.getKey());
                    out.writeLong(edgeEntry.getValue().getTimestamp());
                    encodeString(out, edgeEntry.getValue().getVertexId());
                    encodeText(out, edgeEntry.getValue().getColumnVisibility());
                }
            }
        }
    }

    private static Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> getEdgesByLabel(EdgesWithEdgeInfo edges) throws IOException {
        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = new HashMap<>();
        for (Map.Entry<Text, EdgeInfo> edgeEntry : edges.getEntries()) {
            ByteArrayWrapper label = new ByteArrayWrapper(edgeEntry.getValue().getLabelBytes());
            List<Map.Entry<Text, EdgeInfo>> edgesByLabel = edgesByLabels.get(label);
            if (edgesByLabel == null) {
                edgesByLabel = new ArrayList<>();
                edgesByLabels.put(label, edgesByLabel);
            }
            edgesByLabel.add(edgeEntry);
        }
        return edgesByLabels;
    }

    public static void encodeSetOfStrings(DataOutputStream out, Set<String> strings) throws IOException {
        out.writeInt(strings.size());
        for (String string : strings) {
            encodeString(out, string);
        }
    }

    public static void encodeDirection(DataOutputStream out, Direction direction) throws IOException {
        switch (direction) {
            case IN:
                out.writeByte('I');
                break;
            case OUT:
                out.writeByte('O');
                break;
            default:
                throw new VertexiumAccumuloIteratorException("Unhandled direction: " + direction);
        }
    }

    public static void encodeElementType(DataOutputStream out, ElementType elementType) throws IOException {
        switch (elementType) {
            case VERTEX:
                out.writeByte('V');
                break;
            case EDGE:
                out.writeByte('E');
                break;
            default:
                throw new VertexiumAccumuloIteratorException("Unhandled elementType: " + elementType);
        }
    }
}
