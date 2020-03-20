package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.Direction;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalAddEdgeToVertexEvent extends IteratorHistoricalEvent {
    private final ByteSequence edgeId;
    private final Direction edgeDirection;
    private final byte[] edgeLabelBytes;
    private final byte[] otherVertexIdBytes;
    private final ByteSequence edgeVisibility;

    public IteratorHistoricalAddEdgeToVertexEvent(
        String elementId,
        ByteSequence edgeId,
        Direction edgeDirection,
        byte[] edgeLabelBytes,
        byte[] otherVertexIdBytes,
        ByteSequence edgeVisibility,
        long timestamp
    ) {
        super(ElementType.VERTEX, elementId, timestamp);
        this.edgeId = edgeId;
        this.edgeDirection = edgeDirection;
        this.edgeLabelBytes = edgeLabelBytes;
        this.otherVertexIdBytes = otherVertexIdBytes;
        this.edgeVisibility = edgeVisibility;
    }

    public ByteSequence getEdgeId() {
        return edgeId;
    }

    public Direction getEdgeDirection() {
        return edgeDirection;
    }

    public byte[] getEdgeLabelBytes() {
        return edgeLabelBytes;
    }

    public byte[] getOtherVertexIdBytes() {
        return otherVertexIdBytes;
    }

    public ByteSequence getEdgeVisibility() {
        return edgeVisibility;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence edgeId = DataInputStreamUtils.decodeByteSequence(in);
        Direction edgeDirection = DataInputStreamUtils.decodeDirection(in);
        byte[] edgeLabelBytes = DataInputStreamUtils.decodeByteArray(in);
        byte[] otherVertexIdBytes = DataInputStreamUtils.decodeByteArray(in);
        ByteSequence edgeVisibility = DataInputStreamUtils.decodeByteSequence(in);
        return new IteratorHistoricalAddEdgeToVertexEvent(
            elementId,
            edgeId,
            edgeDirection,
            edgeLabelBytes,
            otherVertexIdBytes,
            edgeVisibility,
            timestamp
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeId());
        DataOutputStreamUtils.encodeDirection(out, getEdgeDirection());
        DataOutputStreamUtils.encodeByteArray(out, getEdgeLabelBytes());
        DataOutputStreamUtils.encodeByteArray(out, getOtherVertexIdBytes());
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeVisibility());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_ADD_EDGE_TO_VERTEX;
    }
}
