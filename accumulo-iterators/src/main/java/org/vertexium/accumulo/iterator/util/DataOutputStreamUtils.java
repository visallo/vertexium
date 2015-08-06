package org.vertexium.accumulo.iterator.util;

import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.EdgesWithEdgeInfo;
import org.vertexium.accumulo.iterator.model.PropertyMetadata;

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

    public static void encodeText(DataOutputStream out, Text text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(text.getLength());
        out.write(text.getBytes(), 0, text.getLength());
    }

    public static void encodeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
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

    public static void encodePropertyMetadata(DataOutputStream out, PropertyMetadata metadata) throws IOException {
        if (metadata == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(metadata.entries.size());
        for (Map.Entry<String, PropertyMetadata.Entry> propertyMetadata : metadata.entries.entrySet()) {
            encodePropertyMetadataEntry(out, propertyMetadata.getValue());
        }
    }

    public static void encodePropertyMetadataEntry(DataOutputStream out, PropertyMetadata.Entry metadataEntry) throws IOException {
        encodeString(out, metadataEntry.metadataKey);
        encodeString(out, metadataEntry.metadataVisibility);
        out.writeInt(metadataEntry.value.length);
        out.write(metadataEntry.value);
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
}
