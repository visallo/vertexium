package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    protected void encode(DataOutputStream out, IteratorFetchHints fetchHints) throws IOException {
        super.encode(out, fetchHints);
        DataOutputStreamUtils.encodeEdges(
                out,
                outEdges,
                fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeOutEdgeRefs()));
        DataOutputStreamUtils.encodeEdges(
                out,
                inEdges,
                fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeInEdgeRefs()));
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_VERTEX;
    }
}
