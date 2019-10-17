package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalSoftDeleteEdgeEvent extends IteratorHistoricalSoftDeleteElementEvent {
    private final ByteSequence outVertexId;
    private final ByteSequence inVertexId;
    private final ByteSequence edgeLabel;

    public IteratorHistoricalSoftDeleteEdgeEvent(
        String elementId,
        ByteSequence outVertexId,
        ByteSequence inVertexId,
        ByteSequence edgeLabel,
        long timestamp,
        Value data
    ) {
        super(ElementType.EDGE, elementId, timestamp, data);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.edgeLabel = edgeLabel;
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

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getOutVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getInVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeLabel());
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    static IteratorHistoricalSoftDeleteEdgeEvent decode(
        DataInputStream in,
        ElementType elementType,
        String elementId,
        long timestamp
    ) throws IOException {
        ByteSequence outVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence inVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence edgeLabel = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalSoftDeleteEdgeEvent(
            elementId,
            outVertexId,
            inVertexId,
            edgeLabel,
            timestamp,
            data
        );
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_SOFT_DELETE_EDGE;
    }
}
