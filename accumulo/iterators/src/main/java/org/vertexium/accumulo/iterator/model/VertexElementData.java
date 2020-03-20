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
    public final IteratorEdgesWithEdgeInfo outEdges = new IteratorEdgesWithEdgeInfo();
    public final IteratorEdgesWithEdgeInfo inEdges = new IteratorEdgesWithEdgeInfo();
    public final Set<Text> hiddenEdges = new HashSet<>();
    public final List<SoftDeleteEdgeInfo> outSoftDeletes = new ArrayList<>();
    public final List<SoftDeleteEdgeInfo> inSoftDeletes = new ArrayList<>();

    @Override
    public void clear(EdgeLabels edgeLabels) {
        super.clear(edgeLabels);
        outEdges.clear(edgeLabels);
        inEdges.clear(edgeLabels);
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
            fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeOutEdgeRefs()),
            fetchHints.isIncludeEdgeIds(),
            fetchHints.isIncludeEdgeVertexIds()
        );
        DataOutputStreamUtils.encodeEdges(
            out,
            inEdges,
            fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeInEdgeRefs()),
            fetchHints.isIncludeEdgeIds(),
            fetchHints.isIncludeEdgeVertexIds()
        );
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_VERTEX;
    }
}
