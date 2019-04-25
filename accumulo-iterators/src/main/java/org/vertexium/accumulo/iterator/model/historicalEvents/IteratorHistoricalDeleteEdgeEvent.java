package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalDeleteEdgeEvent extends IteratorHistoricalDeleteElementEvent {
    private final ByteSequence outVertexId;
    private final ByteSequence inVertexId;
    private final ByteSequence edgeLabel;
    private final ByteSequence visibility;

    public IteratorHistoricalDeleteEdgeEvent(
        String elementId,
        ByteSequence outVertexId,
        ByteSequence inVertexId,
        ByteSequence edgeLabel,
        ByteSequence visibility,
        Long timestamp
    ) {
        super(ElementType.EDGE, elementId, timestamp);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.edgeLabel = edgeLabel;
        this.visibility = visibility;
    }

    public ByteSequence getOutVertexId() {
        return outVertexId;
    }

    public ByteSequence getInVertexId() {
        return inVertexId;
    }

    public ByteSequence getEdgeLabel() {
        return edgeLabel;
    }

    public ByteSequence getVisibility() {
        return visibility;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence outVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence inVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence edgeLabel = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence visibility = DataInputStreamUtils.decodeByteSequence(in);
        return new IteratorHistoricalDeleteEdgeEvent(
            elementId,
            outVertexId,
            inVertexId,
            edgeLabel,
            visibility,
            timestamp
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getOutVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getInVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeLabel());
        DataOutputStreamUtils.encodeByteSequence(out, getVisibility());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_DELETE_EDGE;
    }
}
