package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class EdgeElementData extends ElementData {
    public Text inVertexId;
    public Text outVertexId;
    public Text label;
    public Long inVertexIdTimestamp;
    public Long outVertexIdTimestamp;

    @Override
    public void clear(EdgeLabels edgeLabels) {
        super.clear(edgeLabels);
        inVertexId = null;
        outVertexId = null;
        label = null;
        inVertexIdTimestamp = null;
        outVertexIdTimestamp = null;
    }

    @Override
    protected void encode(DataOutputStream out, IteratorFetchHints fetchHints) throws IOException {
        super.encode(out, fetchHints);
        DataOutputStreamUtils.encodeText(out, inVertexId);
        DataOutputStreamUtils.encodeText(out, outVertexId);
        DataOutputStreamUtils.encodeText(out, label);
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_EDGE;
    }
}
