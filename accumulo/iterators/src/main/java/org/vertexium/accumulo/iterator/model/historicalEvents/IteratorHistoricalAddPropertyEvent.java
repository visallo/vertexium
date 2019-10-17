package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalAddPropertyEvent extends IteratorHistoricalPropertyEvent {
    private final Long previousValueTimestamp;
    private final Value previousValue;
    private final Value value;
    private final IteratorMapMetadata propertyMetadata;

    public IteratorHistoricalAddPropertyEvent(
        ElementType elementType,
        String elementId,
        ByteSequence propertyKey,
        ByteSequence propertyName,
        ByteSequence propertyVisibility,
        Long previousValueTimestamp,
        Value previousValue,
        Value value,
        IteratorMapMetadata propertyMetadata,
        Long propertyTimestamp
    ) {
        super(elementType, elementId, propertyKey, propertyName, propertyVisibility, propertyTimestamp);
        this.previousValueTimestamp = previousValueTimestamp;
        this.previousValue = previousValue;
        this.value = value;
        this.propertyMetadata = propertyMetadata;
    }

    public Long getPreviousValueTimestamp() {
        return previousValueTimestamp;
    }

    public Value getPreviousValue() {
        return previousValue;
    }

    public Value getValue() {
        return value;
    }

    public IteratorMapMetadata getPropertyMetadata() {
        return propertyMetadata;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, ElementType elementType, String elementId, long timestamp) throws IOException {
        ByteSequence propertyKey = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyName = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Long previousValueTimestamp = DataInputStreamUtils.decodeLong(in);
        Value previousValue = DataInputStreamUtils.decodeValue(in);
        Value value = DataInputStreamUtils.decodeValue(in);
        IteratorMapMetadata propertyMetadata = IteratorMapMetadata.decode(in);
        return new IteratorHistoricalAddPropertyEvent(
            elementType,
            elementId,
            propertyKey,
            propertyName,
            propertyVisibility,
            previousValueTimestamp,
            previousValue,
            value,
            propertyMetadata,
            timestamp
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeLong(out, getPreviousValueTimestamp());
        DataOutputStreamUtils.encodeValue(out, getPreviousValue());
        DataOutputStreamUtils.encodeValue(out, getValue());
        getPropertyMetadata().encode(out);
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_ADD_PROPERTY;
    }
}
