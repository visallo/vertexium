package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class VertexElementData extends ElementData {
    public static final byte EDGE_LABEL_ONLY_MARKER = 1;
    public static final byte EDGE_LABEL_WITH_REFS_MARKER = 2;
    public final EdgesWithEdgeInfo outEdges = new EdgesWithEdgeInfo();
    public final EdgesWithEdgeInfo inEdges = new EdgesWithEdgeInfo();
    public final Set<Text> hiddenEdges = new HashSet<>();
    public final List<SoftDeleteEdgeInfo> outSoftDeletes = new ArrayList<>();
    public final List<SoftDeleteEdgeInfo> inSoftDeletes = new ArrayList<>();

    @Override
    public void clear() {
        super.clear();
        outEdges.clear();
        inEdges.clear();
        hiddenEdges.clear();
        outSoftDeletes.clear();
        inSoftDeletes.clear();
    }

    @Override
    protected void encode(DataOutputStream out, EnumSet<FetchHint> fetchHints) throws IOException {
        super.encode(out, fetchHints);
        encodeEdges(out, outEdges, fetchHints.contains(FetchHint.OUT_EDGE_LABELS) && !fetchHints.contains(FetchHint.OUT_EDGE_REFS));
        encodeEdges(out, inEdges, fetchHints.contains(FetchHint.IN_EDGE_LABELS) && !fetchHints.contains(FetchHint.IN_EDGE_REFS));
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_VERTEX;
    }

    private void encodeEdges(DataOutputStream out, EdgesWithEdgeInfo edges, boolean edgeLabelsOnly) throws IOException {
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

    private Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> getEdgesByLabel(EdgesWithEdgeInfo edges) throws IOException {
        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = new HashMap<>();
        for (Map.Entry<Text, EdgeInfo> edgeEntry : edges.getEdges().entrySet()) {
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
}
