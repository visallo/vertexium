package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalAlterEdgeVisibilityEvent extends IteratorHistoricalAlterElementVisibilityEvent {
    private final ByteSequence outVertexId;
    private final ByteSequence inVertexId;
    private final ByteSequence edgeLabel;

    public IteratorHistoricalAlterEdgeVisibilityEvent(
        String elementId,
        ByteSequence outVertexId,
        ByteSequence inVertexId,
        ByteSequence edgeLabel,
        ByteSequence oldVisibility,
        ByteSequence newVisibility,
        Long timestamp,
        Value data
    ) {
        super(ElementType.EDGE, elementId, oldVisibility, newVisibility, timestamp, data);
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

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence outVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence inVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence edgeLabel = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence oldVisibility = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence newVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalAlterEdgeVisibilityEvent(
            elementId,
            outVertexId,
            inVertexId,
            edgeLabel,
            oldVisibility,
            newVisibility,
            timestamp,
            data
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getOutVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getInVertexId());
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeLabel());
        DataOutputStreamUtils.encodeByteSequence(out, getOldVisibility());
        DataOutputStreamUtils.encodeByteSequence(out, getNewVisibility());
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_ALTER_EDGE_VISIBILITY;
    }
}
