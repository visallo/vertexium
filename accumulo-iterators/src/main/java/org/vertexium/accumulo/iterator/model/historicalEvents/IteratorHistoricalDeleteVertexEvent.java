package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalDeleteVertexEvent extends IteratorHistoricalDeleteElementEvent {
    private final ByteSequence visibility;

    public IteratorHistoricalDeleteVertexEvent(
        String elementId,
        ByteSequence visibility,
        Long timestamp
    ) {
        super(ElementType.VERTEX, elementId, timestamp);
        this.visibility = visibility;
    }

    public ByteSequence getVisibility() {
        return visibility;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence visibility = DataInputStreamUtils.decodeByteSequence(in);
        return new IteratorHistoricalDeleteVertexEvent(
            elementId,
            visibility,
            timestamp
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getVisibility());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_DELETE_VERTEX;
    }
}
