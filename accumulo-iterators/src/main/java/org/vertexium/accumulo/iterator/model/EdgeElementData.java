package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;

public class EdgeElementData extends ElementData {
    public Text inVertexId;
    public Text outVertexId;
    public Text label;

    @Override
    public void clear() {
        super.clear();
        inVertexId = null;
        outVertexId = null;
        label = null;
    }

    @Override
    protected void encode(DataOutputStream out, EnumSet<FetchHint> fetchHints) throws IOException {
        super.encode(out, fetchHints);
        encodeText(out, inVertexId);
        encodeText(out, outVertexId);
        encodeText(out, label);
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_EDGE;
    }
}
