package org.vertexium.accumulo.iterator.model;

import com.google.protobuf.TextByteString;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.proto.Edge;

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
    public Value encode(IteratorFetchHints fetchHints) {
        Edge edge = Edge.newBuilder()
            .setInVertexId(new TextByteString(inVertexId))
            .setOutVertexId(new TextByteString(outVertexId))
            .setLabel(new TextByteString(label))
            .setElement(encodeElement(fetchHints))
            .build();
        return new Value(edge.toByteArray());
    }
}
