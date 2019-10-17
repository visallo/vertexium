package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalAlterVertexVisibilityEvent extends IteratorHistoricalAlterElementVisibilityEvent {
    public IteratorHistoricalAlterVertexVisibilityEvent(
        String elementId,
        ByteSequence oldVisibility,
        ByteSequence newVisibility,
        long timestamp,
        Value data
    ) {
        super(ElementType.VERTEX, elementId, oldVisibility, newVisibility, timestamp, data);
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence oldVisibility = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence newVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalAlterVertexVisibilityEvent(
            elementId,
            oldVisibility,
            newVisibility,
            timestamp,
            data
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getOldVisibility());
        DataOutputStreamUtils.encodeByteSequence(out, getNewVisibility());
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_ALTER_VERTEX_VISIBILITY;
    }
}
