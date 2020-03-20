package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.Direction;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalSoftDeleteEdgeToVertexEvent extends IteratorHistoricalEvent {
    private final ByteSequence edgeId;
    private final Direction edgeDirection;
    private final byte[] edgeLabelBytes;
    private final byte[] otherVertexIdBytes;
    private final ByteSequence edgeVisibility;
    private final Value data;

    public IteratorHistoricalSoftDeleteEdgeToVertexEvent(
        String elementId,
        ByteSequence edgeId,
        Direction edgeDirection,
        byte[] edgeLabelBytes,
        byte[] otherVertexIdBytes,
        ByteSequence edgeVisibility,
        long timestamp,
        Value data
    ) {
        super(ElementType.VERTEX, elementId, timestamp);
        this.edgeId = edgeId;
        this.edgeDirection = edgeDirection;
        this.edgeLabelBytes = edgeLabelBytes;
        this.otherVertexIdBytes = otherVertexIdBytes;
        this.edgeVisibility = edgeVisibility;
        this.data = data;
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
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalSoftDeleteEdgeToVertexEvent(
            elementId,
            edgeId,
            edgeDirection,
            edgeLabelBytes,
            otherVertexIdBytes,
            edgeVisibility,
            timestamp,
            data
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
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_SOFT_DELETE_EDGE_TO_VERTEX;
    }

    public Value getData() {
        return data;
    }
}
