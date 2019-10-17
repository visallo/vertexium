package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalSoftDeleteVertexEvent extends IteratorHistoricalSoftDeleteElementEvent {
    public IteratorHistoricalSoftDeleteVertexEvent(String elementId, long timestamp, Value data) {
        super(ElementType.VERTEX, elementId, timestamp, data);
    }

    static IteratorHistoricalSoftDeleteVertexEvent decode(
        DataInputStream in,
        ElementType elementType,
        String elementId,
        long timestamp
    ) throws IOException {
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalSoftDeleteVertexEvent(elementId, timestamp, data);
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_SOFT_DELETE_VERTEX;
    }
}
