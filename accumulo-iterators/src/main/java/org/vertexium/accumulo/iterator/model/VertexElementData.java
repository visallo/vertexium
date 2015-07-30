package org.vertexium.accumulo.iterator.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class VertexElementData extends ElementData {
    public static final byte EDGE_LABEL_ONLY_MARKER = 1;
    public static final byte EDGE_LABEL_WITH_REFS_MARKER = 2;
    public final EdgesWithEdgeInfo outEdges = new EdgesWithEdgeInfo();
    public final EdgesWithEdgeInfo inEdges = new EdgesWithEdgeInfo();
    public final Set<String> hiddenEdges = new HashSet<>();
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

        Map<String, List<Map.Entry<String, EdgeInfo>>> edgesByLabels = getEdgesByLabel(edges);
        out.writeInt(edgesByLabels.size());
        for (Map.Entry<String, List<Map.Entry<String, EdgeInfo>>> entry : edgesByLabels.entrySet()) {
            encodeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            if (!edgeLabelsOnly) {
                for (Map.Entry<String, EdgeInfo> edgeEntry : entry.getValue()) {
                    encodeString(out, edgeEntry.getKey());
                    out.writeLong(edgeEntry.getValue().getTimestamp());
                    encodeString(out, edgeEntry.getValue().getVertexId());
                }
            }
        }
    }

    private Map<String, List<Map.Entry<String, EdgeInfo>>> getEdgesByLabel(EdgesWithEdgeInfo edges) throws IOException {
        Map<String, List<Map.Entry<String, EdgeInfo>>> edgesByLabels = new HashMap<>();
        for (Map.Entry<String, EdgeInfo> edgeEntry : edges.getEdges().entrySet()) {
            String label = edgeEntry.getValue().getLabel();
            List<Map.Entry<String, EdgeInfo>> edgesByLabel = edgesByLabels.get(label);
            if (edgesByLabel == null) {
                edgesByLabel = new ArrayList<>();
                edgesByLabels.put(label, edgesByLabel);
            }
            edgesByLabel.add(edgeEntry);
        }
        return edgesByLabels;
    }
}
