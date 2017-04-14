package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataOutputStream;
import java.io.IOException;
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
    protected void encode(DataOutputStream out, EnumSet<IteratorFetchHint> fetchHints) throws IOException {
        super.encode(out, fetchHints);
        DataOutputStreamUtils.encodeEdges(out, outEdges, fetchHints.contains(IteratorFetchHint.OUT_EDGE_LABELS) && !fetchHints.contains(IteratorFetchHint.OUT_EDGE_REFS));
        DataOutputStreamUtils.encodeEdges(out, inEdges, fetchHints.contains(IteratorFetchHint.IN_EDGE_LABELS) && !fetchHints.contains(IteratorFetchHint.IN_EDGE_REFS));
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_VERTEX;
    }
}
