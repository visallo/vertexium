package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalSoftDeletePropertyEvent extends IteratorHistoricalPropertyEvent {
    private final Value data;

    public IteratorHistoricalSoftDeletePropertyEvent(
        ElementType elementType,
        String elementId,
        ByteSequence propertyKey,
        ByteSequence propertyName,
        ByteSequence propertyVisibility,
        long timestamp,
        Value data
    ) {
        super(elementType, elementId, propertyKey, propertyName, propertyVisibility, timestamp);
        this.data = data;
    }

    public static IteratorHistoricalEvent decode(DataInputStream in, ElementType elementType, String elementId, long timestamp) throws IOException {
        ByteSequence propertyKey = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyName = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalSoftDeletePropertyEvent(
            elementType,
            elementId,
            propertyKey,
            propertyName,
            propertyVisibility,
            timestamp,
            data
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_SOFT_DELETE_PROPERTY;
    }

    public Value getData() {
        return data;
    }
}
