package org.vertexium.accumulo.iterator.model;

import com.google.protobuf.ByteArrayByteString;
import com.google.protobuf.TextByteString;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.proto.*;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;

import java.util.*;

public class VertexElementData extends ElementData {
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
    public Value encode(IteratorFetchHints fetchHints) {
        Vertex.Builder builder = Vertex.newBuilder()
            .setElement(encodeElement(fetchHints));
        if (fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeOutEdgeRefs())) {
            builder.setOutEdgeCounts(encodeEdgeCounts(outEdges));
        } else {
            builder.setOutEdgeRefs(encodeEdgeRefs(outEdges));
        }
        if (fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeInEdgeRefs())) {
            builder.setInEdgeCounts(encodeEdgeCounts(inEdges));
        } else {
            builder.setInEdgeRefs(encodeEdgeRefs(inEdges));
        }
        return new Value(builder.build().toByteArray());
    }

    private EdgeCounts encodeEdgeCounts(EdgesWithEdgeInfo edges) {
        EdgeCounts.Builder edgeCounts = EdgeCounts.newBuilder();
        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = getEdgesByLabel(edges);
        for (Map.Entry<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> entry : edgesByLabels.entrySet()) {
            edgeCounts.addEdges(LabelEdgeCounts.newBuilder()
                .setLabel(new ByteArrayByteString(entry.getKey().getData()))
                .setCount(entry.getValue().size())
                .build());
        }
        return edgeCounts.build();
    }

    private EdgeRefs encodeEdgeRefs(EdgesWithEdgeInfo edges) {
        EdgeRefs.Builder edgeRefs = EdgeRefs.newBuilder();
        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = getEdgesByLabel(edges);
        for (Map.Entry<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> entry : edgesByLabels.entrySet()) {
            byte[] label = entry.getKey().getData();
            List<Map.Entry<Text, EdgeInfo>> edgeData = entry.getValue();
            LabelEdgeRefs.Builder labelEdgeRefs = LabelEdgeRefs.newBuilder()
                .setLabel(new ByteArrayByteString(label));
            for (Map.Entry<Text, EdgeInfo> edgeEntry : edgeData) {
                labelEdgeRefs.addEdgeRef(EdgeRef.newBuilder()
                    .setEdgeId(new TextByteString(edgeEntry.getKey()))
                    .setTimestamp(edgeEntry.getValue().getTimestamp())
                    .setVertexId(edgeEntry.getValue().getVertexId())
                    .build());
            }
            edgeRefs.addEdges(labelEdgeRefs.build());
        }
        return edgeRefs.build();
    }

    private static Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> getEdgesByLabel(EdgesWithEdgeInfo edges) {
        Map<ByteArrayWrapper, List<Map.Entry<Text, EdgeInfo>>> edgesByLabels = new HashMap<>();
        for (Map.Entry<Text, EdgeInfo> edgeEntry : edges.getEntries()) {
            ByteArrayWrapper label = new ByteArrayWrapper(edgeEntry.getValue().getLabelBytes());
            List<Map.Entry<Text, EdgeInfo>> edgesByLabel = edgesByLabels.computeIfAbsent(label, k -> new ArrayList<>());
            edgesByLabel.add(edgeEntry);
        }
        return edgesByLabels;
    }
}
